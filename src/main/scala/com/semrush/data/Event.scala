package com.semrush.data

import java.util.Date

/**
  * Case class represents event from user history e.g user click.
  *
  * @param serverStamp     unix timestamp, when an event was registered
  * @param clientStamp     unix timestamp, when an event occurred
  * @param url             URL, which an user visited
  * @param domain          TLD from url field
  * @param referer         URL from which an user moved (may be absent in case of a direct call)
  * @param refererDomain   TLD of the site, received from referer (may be absent in case of a direct call).
  * @param statusCode      status code
  * @param country_code    country code
  * @param browser         browser
  * @param operatingSystem operation system
  * @param uid             user identifier
  * @param sid             session identifier
  * @author Alexey Boznyakov
  */
case class Event(serverStamp: Date,
                 clientStamp: Date,
                 url: String,
                 domain: String,
                 referer: Option[String],
                 refererDomain: Option[String],
                 statusCode: Int,
                 country_code: String,
                 browser: String,
                 operatingSystem: String,
                 uid: String,
                 sid: String
                )

/**
  * Companion object.
  */
object Event {

  /**
    * Helper method for generating session identifier.
    *
    * @param uid        user identifier
    * @param domain     session domain
    * @param startStamp time, when a session started (actually it's time of first session event)
    * @return session id
    */
  def makeSid(uid: String, domain: String, startStamp: Date): String =
    startStamp.getTime.toString + (uid, domain).hashCode
}
