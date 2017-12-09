package com.semrush.data

import java.sql.Timestamp

import com.semrush.{TestSuite, data}
import org.scalatest.FlatSpec

/**
  * Class is intended to test class [[com.semrush.data.DataProcessor]].
  */
class DataProcessorTest extends FlatSpec {

  /**
    * Input data.
    */
  val stringList: List[String] = List[String](
    """{"server_stamp":1506816287000,"client_stamp":1506816166795,"url":"https://www.kurnik.pl/pilka/","domain":"kurnik.pl","referer":"https://www.kurnik.pl/pilka/","referer_domain":"kurnik.pl","status_code":200,"country_code":"GB","browser":"Chrome","operating_system":"Windows7","uid":"5e1d8d214ed4c39ce19f8351963bfa9c"}""",

    """{"server_stamp":1506816311000,"client_stamp":1506816191913,"url":"https://www.panynj.gov/airports/ewr-to-from.html","domain":"panynj.gov","referer":"https://www.google.ca/","referer_domain":"google.ca","status_code":200,"country_code":"CA","browser":"Chrome","operating_system":"Windows8.1","uid":"f7ba9b9aacd447ecc0b4d59de22ff76d"}""",

    """{"server_stamp":1506816311000,"client_stamp":1506816167698,"url":"https://www.kurnik.pl/pilka/#102","domain":"kurnik.pl","referer":"https://www.kurnik.pl/pilka/","referer_domain":"kurnik.pl","status_code":200,"country_code":"GB","browser":"Chrome","operating_system":"Windows7","uid":"5e1d8d214ed4c39ce19f8351963bfa9c"}""",

    """{"server_stamp":1506816311000,"client_stamp":1506816241800,"url":"https://www.google.ca/search?q=newark+airport+taxi+credit+card","domain":"google.ca","status_code":200,"country_code":"CA","browser":"Chrome","operating_system":"Windows8.1","uid":"f7ba9b9aacd447ecc0b4d59de22ff76d"}""",

    """{"server_stamp":1506816311000,"client_stamp":1506816244646,"url":"https://www.google.ca/url?sa=t&rct=j&q=&esrc=s&source=web&cd=1&url=https%3A%2F%2Fwww.tripadvisor.ca%2FShowTopic-g46671-i866-k4524223-Credit_card_to_pay_taxi_ride-Newark_New_Jersey.html&usg=AFQjCNGwhsq959fr9_YFYtYLBsAgX7iOHQ","domain":"google.ca","referer":"https://www.google.ca/","referer_domain":"google.ca","status_code":200,"country_code":"CA","browser":"Chrome","operating_system":"Windows8.1","uid":"f7ba9b9aacd447ecc0b4d59de22ff76d"}""",

    """{"server_stamp":1506816311000,"client_stamp":1506816245632,"url":"https://www.tripadvisor.ca/ShowTopic-g46671-i866-k4524223-Credit_card_to_pay_taxi_ride-Newark_New_Jersey.html","domain":"tripadvisor.ca","referer":"https://www.google.ca/","referer_domain":"google.ca","status_code":200,"country_code":"CA","browser":"Chrome","operating_system":"Windows8.1","uid":"f7ba9b9aacd447ecc0b4d59de22ff76d"}""",

    """{"server_stamp":1506816312000,"client_stamp":1506816172282,"url":"https://www.kurnik.pl/pilka/","domain":"kurnik.pl","referer":"https://google.com/","referer_domain":"google.com","status_code":200,"country_code":"GB","browser":"Chrome","operating_system":"Windows7","uid":"5e1d8d214ed4c39ce19f8351963bfa9c"}""",

    """{"server_stamp":1506816313000,"client_stamp":1506816331663,"url":"https://www.kurnik.pl/pilka/","domain":"kurnik.pl","status_code":200,"country_code":"GB","browser":"Chrome","operating_system":"Windows7","uid":"5e1d8d214ed4c39ce19f8351963bfa9c"}""",

    """{"server_stamp":1506818711000,"client_stamp":1506818645374,"url":"https://www.tripadvisor.ca","domain":"tripadvisor.ca","referer":"https://www.tripadvisor.ca/ShowTopic-g46671-i866-k4524223-Credit_card_to_pay_taxi_ride-Newark_New_Jersey.html","referer_domain":"tripadvisor.ca","status_code":200,"country_code":"CA","browser":"Chrome","operating_system":"Windows8.1","uid":"f7ba9b9aacd447ecc0b4d59de22ff76d"}"""
  )

  /**
    * Expected result list.
    */
  val eventList: List[Event] = List[Event](
    // *** SESSION ***
    Event(
      serverStamp = new Timestamp(1506816287000L),
      clientStamp = new Timestamp(1506816166795L), // take this timestamp as session start
      url = """https://www.kurnik.pl/pilka/""",
      domain = "kurnik.pl",
      referer = Some[String]("""https://www.kurnik.pl/pilka/"""),
      refererDomain = Some[String]("kurnik.pl"),
      statusCode = 200,
      country_code = "GB",
      browser = "Chrome",
      operatingSystem = "Windows7",
      uid = "5e1d8d214ed4c39ce19f8351963bfa9c",
      sid = data.Event.makeSid("5e1d8d214ed4c39ce19f8351963bfa9c", "kurnik.pl", new Timestamp(1506816166795L))
    ),
    Event(
      serverStamp = new Timestamp(1506816311000L),
      clientStamp = new Timestamp(1506816167698L), // take this timestamp as session start
      url = """https://www.kurnik.pl/pilka/#102""",
      domain = "kurnik.pl",
      referer = Some[String]("""https://www.kurnik.pl/pilka/"""),
      refererDomain = Some[String]("kurnik.pl"),
      statusCode = 200,
      country_code = "GB",
      browser = "Chrome",
      operatingSystem = "Windows7",
      uid = "5e1d8d214ed4c39ce19f8351963bfa9c",
      sid = data.Event.makeSid("5e1d8d214ed4c39ce19f8351963bfa9c", "kurnik.pl", new Timestamp(1506816166795L))
    ),
    // *** SESSION ***
    Event(
      serverStamp = new Timestamp(1506816311000L),
      clientStamp = new Timestamp(1506816191913L),
      url = """https://www.panynj.gov/airports/ewr-to-from.html""",
      domain = "panynj.gov",
      referer = Some[String]("""https://www.google.ca/"""),
      refererDomain = Some[String]("google.ca"),
      statusCode = 200,
      country_code = "CA",
      browser = "Chrome",
      operatingSystem = "Windows8.1",
      uid = "f7ba9b9aacd447ecc0b4d59de22ff76d",
      sid = data.Event.makeSid("f7ba9b9aacd447ecc0b4d59de22ff76d", "panynj.gov", new Timestamp(1506816191913L))
    ),
    // *** SESSION ***
    Event(
      serverStamp = new Timestamp(1506816311000L),
      clientStamp = new Timestamp(1506816241800L), // take this timestamp as session start
      url = """https://www.google.ca/search?q=newark+airport+taxi+credit+card""",
      domain = "google.ca",
      referer = None,
      refererDomain = None,
      statusCode = 200,
      country_code = "CA",
      browser = "Chrome",
      operatingSystem = "Windows8.1",
      uid = "f7ba9b9aacd447ecc0b4d59de22ff76d",
      sid = data.Event.makeSid("f7ba9b9aacd447ecc0b4d59de22ff76d", "google.ca", new Timestamp(1506816241800L))
    ),
    Event(
      serverStamp = new Timestamp(1506816311000L),
      clientStamp = new Timestamp(1506816244646L),
      url = """https://www.google.ca/url?sa=t&rct=j&q=&esrc=s&source=web&cd=1&url=https%3A%2F%2Fwww.tripadvisor.ca%2FShowTopic-g46671-i866-k4524223-Credit_card_to_pay_taxi_ride-Newark_New_Jersey.html&usg=AFQjCNGwhsq959fr9_YFYtYLBsAgX7iOHQ""",
      domain = "google.ca",
      referer = Some[String]("https://www.google.ca/"),
      refererDomain = Some[String]("google.ca"),
      statusCode = 200,
      country_code = "CA",
      browser = "Chrome",
      operatingSystem = "Windows8.1",
      uid = "f7ba9b9aacd447ecc0b4d59de22ff76d",
      sid = data.Event.makeSid("f7ba9b9aacd447ecc0b4d59de22ff76d", "google.ca", new Timestamp(1506816241800L))
    ),
    // *** SESSION ***
    Event(
      serverStamp = new Timestamp(1506816311000L),
      clientStamp = new Timestamp(1506816245632L),
      url = """https://www.tripadvisor.ca/ShowTopic-g46671-i866-k4524223-Credit_card_to_pay_taxi_ride-Newark_New_Jersey.html""",
      domain = "tripadvisor.ca",
      referer = Some[String]("""https://www.google.ca/"""),
      refererDomain = Some[String]("google.ca"),
      statusCode = 200,
      country_code = "CA",
      browser = "Chrome",
      operatingSystem = "Windows8.1",
      uid = "f7ba9b9aacd447ecc0b4d59de22ff76d",
      sid = data.Event.makeSid("f7ba9b9aacd447ecc0b4d59de22ff76d", "tripadvisor.ca", new Timestamp(1506816245632L))
    ),
    // *** SESSION ***
    Event(
      serverStamp = new Timestamp(1506816312000L),
      clientStamp = new Timestamp(1506816172282L), // take this timestamp as session start
      url = """https://www.kurnik.pl/pilka/""",
      domain = "kurnik.pl",
      referer = Some[String]("""https://google.com/"""),
      refererDomain = Some[String]("google.com"),
      statusCode = 200,
      country_code = "GB",
      browser = "Chrome",
      operatingSystem = "Windows7",
      uid = "5e1d8d214ed4c39ce19f8351963bfa9c",
      sid = data.Event.makeSid("5e1d8d214ed4c39ce19f8351963bfa9c", "kurnik.pl", new Timestamp(1506816172282L))
    ),
    Event(
      serverStamp = new Timestamp(1506816313000L),
      clientStamp = new Timestamp(1506816331663L),
      url = """https://www.kurnik.pl/pilka/""",
      domain = "kurnik.pl",
      referer = None,
      refererDomain = None,
      statusCode = 200,
      country_code = "GB",
      browser = "Chrome",
      operatingSystem = "Windows7",
      uid = "5e1d8d214ed4c39ce19f8351963bfa9c",
      sid = data.Event.makeSid("5e1d8d214ed4c39ce19f8351963bfa9c", "kurnik.pl", new Timestamp(1506816172282L))
    ),
    // *** SESSION ***
    Event(
      serverStamp = new Timestamp(1506818711000L),
      clientStamp = new Timestamp(1506818645374L), // new session, because previous event was 40 min ago
      url = """https://www.tripadvisor.ca""",
      domain = "tripadvisor.ca",
      referer = Some[String]("""https://www.tripadvisor.ca/ShowTopic-g46671-i866-k4524223-Credit_card_to_pay_taxi_ride-Newark_New_Jersey.html"""),
      refererDomain = Some[String]("tripadvisor.ca"),
      statusCode = 200,
      country_code = "CA",
      browser = "Chrome",
      operatingSystem = "Windows8.1",
      uid = "f7ba9b9aacd447ecc0b4d59de22ff76d",
      sid = data.Event.makeSid("f7ba9b9aacd447ecc0b4d59de22ff76d", "tripadvisor.ca", new Timestamp(1506818645374L))
    )
  )

  "events" should "be tagged with session id" in {
    val sc = TestSuite.sc
    val rawRDD = sc.parallelize(stringList)
    val resultSet: List[data.Event] = data.DataProcessor(rawRDD).collect.toList

    val eventKey = (e: data.Event) => (e.sid, e.uid, e.clientStamp, e.url, e.referer)

    assert(eventList.sortBy(eventKey) == resultSet.sortBy(eventKey)) // sort to be sure, that lists are equals
  }

}
