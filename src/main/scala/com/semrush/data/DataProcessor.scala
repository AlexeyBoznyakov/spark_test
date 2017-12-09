package com.semrush.data

import java.sql.Timestamp
import java.util.Date

import org.apache.spark.rdd.RDD
import org.json4s.JsonAST.{JObject, JValue}
import org.json4s.jackson.{JsonMethods, Serialization}
import org.json4s.{CustomSerializer, Formats, NoTypeHints}

import scala.collection.mutable.ArrayBuffer

/**
  * Class is intended to process events from user.
  *
  * @author Alexey Boznyakov
  */
object DataProcessor {

  /**
    * Session timeout - 30 minutes.
    */
  private val SESSION_TIMEOUT: Int = 30 * 60 * 1000

  /**
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
    * Convert JSON string to pair (key - event.uid concat event.domain, value - [[com.semrush.data.Event]])
    *
    * @param jsonStr JSON string
    * @return map, which represents event [key - event field name, value - event field value]
    */
  private def convertToPair(jsonStr: String): ((String, Event)) = {
    val event = JsonMethods.parse(jsonStr).extract[Event]
    (event.uid + event.domain, event)
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
