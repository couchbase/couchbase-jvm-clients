/*
 * Copyright (c) 2019 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */ /*
 * Copyright (c) 2016 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.scala.search.util

import java.text.{DateFormat, ParseException, SimpleDateFormat}
import java.util.{Date, TimeZone}
import java.util.regex.Pattern

import com.couchbase.client.core.annotation.Stability
import com.couchbase.client.core.error.CouchbaseException

/**
  * Utility class around FTS (Full Text Search), and especially handling of the default FTS date format
  * (which corresponds to RFC 3339).
  *
  * @since 1.0.0
  */
private[scala] object SearchUtils {
  // Implementation note: this is a dumb auto-convert of the Java code to Scala, there's doubtless room for
  // improvement

  private val FTS_SIMPLE_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ssZZZZ"
  private val PATTERN                = Pattern.compile("([\\+-])(\\d{1,2})(?::(\\d{1,2}))?$")

  private val df = new ThreadLocal[DateFormat]() {
    override protected def initialValue: DateFormat = {
      val sdf = new SimpleDateFormat(FTS_SIMPLE_DATE_FORMAT)
      sdf.setTimeZone(TimeZone.getTimeZone("UTC"))
      sdf.setLenient(false)
      sdf
    }
  }

  /**
    * Converts a date to the default string representation in FTS (RFC 3339).
    *
    * @param date the [[Date]] to convert.
    * @return the RFC 3339 representation of the date, in UTC timezone (eg. "2016-01-19T14:44:01Z")
    */
  def toFtsUtcString(date: Date): String = {
    if (date == null) return null
    val rfc3339 = df.get
    val zDate   = rfc3339.format(date)
    val xDate   = zDate.replaceFirst("\\+0000$", "Z")
    xDate
  }

  /**
    * Attempts to convert a date string, as returned by the FTS service, into a {@link Date}.
    * <p>
    * The FTS service is expected to return date strings in RFC 3339 format, including the timezone information.
    * For example: <code>2016-01-10T14:44:01-08:00</code> for a date in the UTC-8/PDT timezone.
    *
    * @param date the date in RFC 3339 format.
    *
    * @return the corresponding { @link Date}.
    * @throws CouchbaseException when the date could not be parsed.
    */
  def fromFtsString(date: String): Date = {
    if (date == null) return null
    val zDate = iso822TimezoneToRfc3339Timezone(date)
    try df.get.parse(zDate)
    catch {
      case e: ParseException =>
        throw new CouchbaseException(
          "Cannot parse FTS date '" + date + "' despite convertion to RFC 822 timezone '"
            + zDate + "'",
          e
        )
    }
  }

  private def iso822TimezoneToRfc3339Timezone(xDate: String): String = {
    var zDate: String = null
    if (xDate.endsWith("Z")) zDate = xDate.replaceFirst("Z$", "+0000")
    else {
      val matcher = PATTERN.matcher(xDate)
      if (matcher.find) {
        val sign    = matcher.group(1)
        var hours   = matcher.group(2)
        var minutes = "00"
        if (matcher.groupCount == 3 && matcher.group(3) != null) minutes = matcher.group(3)
        if (hours.length == 1) hours = "0" + hours
        if (minutes.length == 1) minutes = "0" + minutes
        zDate = matcher.replaceFirst(sign + hours + minutes)
      } else throw new CouchbaseException("Cannot convert timezone to RFC 822 in '" + xDate + "'")
    }
    zDate
  }
}
