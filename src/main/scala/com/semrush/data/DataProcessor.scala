package com.semrush.data

import java.sql.Timestamp
import java.util.{Calendar, Date}

import org.apache.commons.lang.time.DateUtils
import org.apache.spark.rdd.RDD
import org.json4s.JsonAST.{JObject, JValue}
import org.json4s.jackson.{JsonMethods, Serialization}
import org.json4s.{CustomSerializer, Formats, NoTypeHints}

import scala.collection.mutable.ArrayBuffer

/**
  * Class is intended to process events from user (assign session id).
  * <p>
  * Session id assigning rules:
  * 1. One session can only include events from the same user for the same domain.
  * 2. Events in the session are sorted by the user's time, and not by the time of the server.
  * 3. The user session is terminated in the following cases:
  * 3.1. The user went to another site (the event is different from the session domain). In this case, a
  * new session for another site begins.
  * 3.2. The user went to the same site (the event and the domain sessions coincide), but the transition was made from
  * another site (referer_domain is defined and differs from the domain session).
  * 3.3. The interval between the events of the session is more than 30 minutes.
  * 4. The user session is not terminated, and the event is considered to belong to the same session if:
  * 4.1. The user visited the same site by a direct link (the event and the domain session are the same, and there
  * is no referer_domain for the event).
  * 4.2. The user entered the site by the link from the same site (the event and the domain sessions coincide,
  * and the referer_domain in the event coincides with the domain session).
  *
  * @author Alexey Boznyakov
  */
object DataProcessor {

  /**
    * Session timeout (in milliseconds) - 30 minutes.
    */
  private val SESSION_TIMEOUT: Int = 30 * 60 * 1000

  /**
    * Function is intended to generate RDD of events marked by session id (sid).
    *
    * <p>
    * Implementation notes: <br>
    * 1. Instead of using groupBy function, aggregateByKey is used(inside "aggregateByKey" "combineByKey" is used).
    * The main advantage is that:
    * Spark executes operation "map-reduce" in each node for different keys and after that sends this data to a
    * certain nodes(according partitioner). On this node Spark combines reduces data from different nodes
    * (using combine function).
    * For example:
    * {{{
    * Node 1                                 Node 2
    * (1, data1)                             (1, data3)
    * (1, data2)                             (1, data4)
    * (2, data1)                             (2, data2)
    *
    * Mad-Reduce
    * (1, [data1, data2]                     (1, [data3, data4]
    * (2, [data1]                            (2, [data2]
    *
    * Shuffle data
    * (1, [data1, data2, data3, data 4]      (2, [data2, data1]
    * }}}
    *
    * GroupBy function sends all data(for a certain key) to certain node and after that executes "map-reduce" operation.
    * It can lead to OutOfMemoryException and heavy network traffic between nodes.
    * <p>
    * 2. For marking every event with SID, algorithm requires SORTED list of events for each key: domain + uid, because
    * there are some rules for SID generation(e.g. session is terminated after 30 min delay - last event required). <br>
    * Imagine that we process data from one year: <br>
    * 365 * 24 * 3600 (one click per second per user) = 32 mln <br>
    * It's very hard to sort and process 32 mln events on ONE node. According this the following decision was made: <br>
    * Usually every user works before midnight. Nothing bad will happen if a session is terminated (false rule triggering) <br>
    * at midnight (or after month, decade etc) This rule can be written to the product documentation. <br>
    * As a result, events for certain days will be processed on different nodes (higher level of parallelism) <br>
    * Anyway if it's impossible(strong business rules) new cached mechanism is required(last event required between
    * two executions of spark job) <br>
    * This implementation terminates session after midnight: <br>
    * In [[DataProcessor.convertToPair]] JSON data converted to pair: <br>
    * key - uid + domain + event date(without time) <br>
    * value - event data
    * <p>
    * 3. Default partitioner (HashPartitioner) was chosen, because there isn't information about events period or number
    * of users.
    * For example:
    * If data consist of events from one year, developer can choose RangePartitioner with 356 partitions.
    *
    * @param rawRDD Raw RDD
    * @return RDD, which represent events from user with sid(session identifier) field.
    */
  def apply(rawRDD: RDD[String]): RDD[Event] = {
    rawRDD.map(str => convertToPair(str))
      .aggregateByKey(ArrayBuffer[Event]())((a, b) => a += b, (a, b) => a ++= b)
      .flatMapValues(group => {
        val sortedEvents = group.sortBy(v => v.clientStamp)

        // generate initial sid
        val firstEvent = sortedEvents(0)
        val initialSid = Event.makeSid(firstEvent.uid, firstEvent.domain, firstEvent.clientStamp)

        // initial value for foldLeft is tuple(emptyList, timeOfLastEvent, initialSid)
        sortedEvents.foldLeft((List[Event](), firstEvent.clientStamp, initialSid))((acc, cEvent) => {
          var currentSid = acc._3
          if (isSessionComplete(cEvent, acc._2)) {
            currentSid = Event.makeSid(cEvent.uid, cEvent.domain, cEvent.clientStamp)
          }
          (cEvent.copy(sid = currentSid) :: acc._1, cEvent.clientStamp, currentSid)
        })._1
      }).values
  }

  /**
    * Check - is a session complete.
    *
    * @param event           current event
    * @param timeOfLastEvent time of last event
    * @return is a session complete (true - yes, false - no)
    */
  private def isSessionComplete(event: Event, timeOfLastEvent: Date): Boolean = {
    val domain = event.refererDomain
    (domain.isDefined && !event.domain.equals(domain.get)) ||
      (event.clientStamp.getTime - timeOfLastEvent.getTime > SESSION_TIMEOUT)
  }

  /**
    * Implicit object for converting JSON string to [[com.semrush.data.Event]].
    */
  private implicit val formats: Formats = {
    Serialization.formats(NoTypeHints) + EventSerializer
  }

  /**
    * Convert JSON string to pair (key - event.uid concat event.domain concat event date,
    * value - [[com.semrush.data.Event]])
    *
    * @param jsonStr JSON string
    * @return map, which represents event [key - event field name, value - event field value]
    */
  private def convertToPair(jsonStr: String): ((String, Event)) = {
    val event = JsonMethods.parse(jsonStr).extract[Event]
    val eventDate = DateUtils.truncate(event.clientStamp, Calendar.DATE)
    (event.uid + event.domain + eventDate, event)
  }

  /**
    * Custom deserializer for converting JSON string to Event.
    * Serialization not supported.
    *
    * @author Alexey Boznyakov
    */
  private object EventSerializer extends CustomSerializer[Event](_ => ( {
    case jObj: JObject =>
      val jsonValues = jObj.extract[Map[String, JValue]]

      val referer = jsonValues.get("referer")
      val refererDomain = jsonValues.get("referer_domain")
      Event(
        new Timestamp(jsonValues("server_stamp").extract[Long]),
        new Timestamp(jsonValues("client_stamp").extract[Long]),
        jsonValues("url").extract[String],
        jsonValues("domain").extract[String],
        Option[String](if (referer.isDefined) referer.get.extract[String] else null),
        Option[String](if (refererDomain.isDefined) refererDomain.get.extract[String] else null),
        jsonValues("status_code").extract[Int],
        jsonValues("country_code").extract[String],
        jsonValues("browser").extract[String],
        jsonValues("operating_system").extract[String],
        jsonValues("uid").extract[String],
        null // sid will be marked later
      )
  }, {
    case Event =>
      throw new UnsupportedOperationException("Serialization not supported")
  }
  ))

}
