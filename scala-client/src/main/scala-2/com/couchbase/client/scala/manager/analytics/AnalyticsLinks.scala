/*
 * Copyright (c) 2021 Couchbase, Inc.
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
package com.couchbase.client.scala.manager.analytics
import com.couchbase.client.scala.util.CouchbasePickler

import scala.util.{Failure, Success, Try}

/** An abstraction over the various forms of analytics links. */
sealed trait AnalyticsLink {
  private[scala] def toMap: Try[Map[String, String]]

  private[scala] def linkType: AnalyticsLinkType

  /** The name of this link. */
  def name: String

  /** The name of the dataverse this link is on. */
  def dataverseName: String
}

object AnalyticsLink {
  implicit val rw: CouchbasePickler.ReadWriter[AnalyticsLink] = CouchbasePickler
    .readwriter[ujson.Obj]
    .bimap[AnalyticsLink](
      (x: AnalyticsLink) => ???, // Unused
      (json) => {
        json("type").str match {
          case "couchbase" => CouchbasePickler.read[CouchbaseRemoteAnalyticsLink](json)
          case "s3"        => CouchbasePickler.read[S3ExternalAnalyticsLink](json)
          case x           => throw new IllegalStateException(s"Cannot decode analytics link type $x")
        }
      }
    )

  /** An analytics link to a remote Couchbase cluster. */
  case class CouchbaseRemoteAnalyticsLink(
      dataverse: String,
      name: String,
      hostname: String,
      encryption: AnalyticsEncryptionLevel
  ) extends AnalyticsLink {
    private[scala] def linkType: AnalyticsLinkType = AnalyticsLinkType.CouchbaseRemote

    override private[scala] def toMap: Try[Map[String, String]] = {
      encryption.toMap
        .map(
          v =>
            v ++
              Map(
                "type"      -> "couchbase",
                "dataverse" -> dataverse,
                "name"      -> name,
                "hostname"  -> hostname,
                "type"      -> "couchbase"
              )
        )
    }

    override def dataverseName: String = dataverse
  }

  private[scala] object CouchbaseRemoteAnalyticsLink {
    implicit val rw: CouchbasePickler.ReadWriter[CouchbaseRemoteAnalyticsLink] = CouchbasePickler
      .readwriter[ujson.Obj]
      .bimap[CouchbaseRemoteAnalyticsLink](
        (x: CouchbaseRemoteAnalyticsLink) => ???, // Unused
        (json) => {
          val dv = if (json.obj.contains("dataverse")) json("dataverse").str else json("scope").str
          CouchbaseRemoteAnalyticsLink(
            dv,
            json("name").str,
            json("activeHostname").str,
            CouchbasePickler.read[AnalyticsEncryptionLevel](json)
          )
        }
      )
  }

  /** An analytics link to S3. */
  case class S3ExternalAnalyticsLink(
      dataverse: String,
      name: String,
      accessKeyID: String,
      secretAccessKey: String,
      sessionToken: String,
      region: String,
      serviceEndpoint: String
  ) extends AnalyticsLink {
    private[scala] def linkType: AnalyticsLinkType = AnalyticsLinkType.S3External

    override private[scala] def toMap: Try[Map[String, String]] = {

      Success(
        Map(
          "type"            -> "s3",
          "dataverse"       -> dataverse,
          "name"            -> name,
          "accessKeyId"     -> accessKeyID,
          "secretAccessKey" -> secretAccessKey,
          "sessionToken"    -> sessionToken,
          "region"          -> region,
          "serviceEndpoint" -> serviceEndpoint,
          "type"            -> "s3"
        )
      )
    }

    override def dataverseName: String = dataverse
  }

  private[scala] object S3ExternalAnalyticsLink {
    implicit val rw: CouchbasePickler.ReadWriter[S3ExternalAnalyticsLink] = CouchbasePickler
      .readwriter[ujson.Obj]
      .bimap[S3ExternalAnalyticsLink](
        (x: S3ExternalAnalyticsLink) => ???, // Unused
        (json) => {
          val dv = if (json.obj.contains("dataverse")) json("dataverse").str else json("scope").str
          // As per RFC, some fields are blanked out
          S3ExternalAnalyticsLink(
            dv,
            json("name").str,
            json("accessKeyId").str,
            "",
            "",
            json("region").str,
            json("serviceEndpoint").str
          )
        }
      )
  }

}

/** Identifies the type of the analytics link.
  */
sealed trait AnalyticsLinkType {
  private[scala] def encode: String
}

object AnalyticsLinkType {
  case object S3External extends AnalyticsLinkType {
    override private[scala] def encode = "s3"
  }

  case object CouchbaseRemote extends AnalyticsLinkType {
    override private[scala] def encode = "couchbase"
  }
}

/** Abstracts over the various encryption levels for connecting to a remote cluster with an analytics link. */
sealed trait AnalyticsEncryptionLevel {
  private[scala] def toMap: Try[Map[String, String]]
}

case class UsernameAndPassword(username: String, password: String) {
  private[scala] def toMap: Try[Map[String, String]] = {
    Success(Map("username" -> username, "password" -> password))
  }
}

object UsernameAndPassword {
  implicit val rw: CouchbasePickler.ReadWriter[UsernameAndPassword] = CouchbasePickler.macroRW
}

case class AnalyticsClientCertificate(clientCertificate: String, clientKey: String) {
  private[scala] def toMap: Try[Map[String, String]] = {
    Success(Map("clientCertificate" -> clientCertificate, "clientKey" -> clientKey))
  }
}

object AnalyticsClientCertificate {
  implicit val rw: CouchbasePickler.ReadWriter[AnalyticsClientCertificate] =
    CouchbasePickler.macroRW
}

object AnalyticsEncryptionLevel {
  implicit val rw: CouchbasePickler.ReadWriter[AnalyticsEncryptionLevel] = CouchbasePickler
    .readwriter[ujson.Obj]
    .bimap[AnalyticsEncryptionLevel](
      (x: AnalyticsEncryptionLevel) => ???, // Unused
      (json) => {
        // As per RFC, some properties are blanked out on reading
        val auth = UsernameAndPassword(json("username").str, "")
        json("encryption").str match {
          case "none" => None(auth)
          case "half" => Half(auth)
          case "full" =>
            None(UsernameAndPassword(json("username").str, json("password").str))
            val clientCertificate = json("clientCertificate").str
            val clientKey         = json("clientKey").str
            if (clientCertificate != null && clientKey != null) {
              Full(
                json("certificate").str,
                Right(AnalyticsClientCertificate(clientCertificate, ""))
              )
            } else {
              Full(json("certificate").str, Left(auth))
            }
          case x => throw new IllegalStateException(s"Cannot decode analytics encryption $x")
        }
      }
    )

  case object Unavailable extends AnalyticsEncryptionLevel {
    private[scala] def toMap: Try[Map[String, String]] = {
      Failure(
        new IllegalStateException("Cannot use the result of getAllLinks directly with replaceLinks")
      )
    }
  }

  case class None(auth: UsernameAndPassword) extends AnalyticsEncryptionLevel {
    private[scala] def toMap: Try[Map[String, String]] = {
      auth.toMap.map(v => v ++ Map("encryption" -> "none"))
    }
  }

  case class Half(auth: UsernameAndPassword) extends AnalyticsEncryptionLevel {
    private[scala] def toMap: Try[Map[String, String]] = {
      auth.toMap.map(v => v ++ Map("encryption" -> "half"))
    }
  }

  case class Full(
      certificate: String,
      auth: Either[UsernameAndPassword, AnalyticsClientCertificate]
  ) extends AnalyticsEncryptionLevel {
    private[scala] def toMap: Try[Map[String, String]] = {
      (auth match {
        case Left(v)  => v.toMap
        case Right(v) => v.toMap
      }).map(v => v ++ Map("encryption" -> "full", "certificate" -> certificate))
    }
  }
}

sealed trait AnalyticsDataType

object AnalyticsDataType {
  case object AnalyticsString extends AnalyticsDataType

  case object AnalyticsInt64 extends AnalyticsDataType

  case object AnalyticsDouble extends AnalyticsDataType
}
