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
 */
package com.couchbase.client.scala.manager.bucket

import com.couchbase.client.core.annotation.Stability.{Internal, Volatile}
import com.couchbase.client.scala.manager.bucket.BucketType.{Couchbase, Ephemeral, Memcached}
import com.couchbase.client.scala.manager.bucket.EjectionMethod.{FullEviction, ValueOnly}
import com.couchbase.client.scala.manager.user.AuthDomain.{External, Local}
import com.couchbase.client.scala.manager.user._
import com.couchbase.client.scala.util.CouchbasePickler

import scala.util.Try

@Volatile
sealed trait BucketType {
  def alias: String
}

object BucketType {

  case object Couchbase extends BucketType {
    override def alias: String = "membase"
  }

  case object Memcached extends BucketType {
    override def alias: String = "memcached"
  }

  case object Ephemeral extends BucketType {
    override def alias: String = "ephemeral"
  }

  implicit val rw: CouchbasePickler.ReadWriter[BucketType] = CouchbasePickler
    .readwriter[String]
    .bimap[BucketType](
      x => x.alias,
      str =>
        str match {
          case "membase"   => Couchbase
          case "memcached" => Memcached
          case "ephemeral" => Ephemeral
        }
    )
}
@Volatile
sealed trait EjectionMethod {
  def alias: String
}

object EjectionMethod {

  case object FullEviction extends EjectionMethod {
    override def alias: String = "fullEviction"
  }

  case object ValueOnly extends EjectionMethod {
    override def alias: String = "valueOnly"
  }

  implicit val rw: CouchbasePickler.ReadWriter[EjectionMethod] = CouchbasePickler
    .readwriter[String]
    .bimap[EjectionMethod](
      x => x.alias,
      str =>
        str match {
          case "fullEviction" => FullEviction
          case "valueOnly"    => ValueOnly
        }
    )
}

@Volatile
sealed trait CompressionMode {
  def alias: String
}

object CompressionMode {

  case object Off extends CompressionMode {
    override def alias: String = "off"
  }

  case object Passive extends CompressionMode {
    override def alias: String = "passive"
  }

  case object Active extends CompressionMode {
    override def alias: String = "active"
  }

  implicit val rw: CouchbasePickler.ReadWriter[CompressionMode] = CouchbasePickler
    .readwriter[String]
    .bimap[CompressionMode](
      x => x.alias,
      str =>
        str match {
          case "off"     => Off
          case "passive" => Passive
          case "active"  => Active
        }
    )

}

@Volatile
sealed trait ConflictResolutionType {
  def alias: String
}

object ConflictResolutionType {

  case object Timestamp extends ConflictResolutionType {
    override def alias: String = "lww"
  }

  case object SequenceNumber extends ConflictResolutionType {
    override def alias: String = "seqno"
  }

  implicit val rw: CouchbasePickler.ReadWriter[ConflictResolutionType] = CouchbasePickler
    .readwriter[String]
    .bimap[ConflictResolutionType](
      x => x.alias,
      str =>
        str match {
          case "lww"   => Timestamp
          case "seqno" => SequenceNumber
        }
    )
}
@Volatile
case class CreateBucketSettings(
    private[scala] val name: String,
    private[scala] val ramQuotaMB: Int,
    private[scala] val flushEnabled: Option[Boolean] = None,
    private[scala] val numReplicas: Option[Int] = None,
    private[scala] val replicaIndexes: Option[Boolean] = None,
    private[scala] val bucketType: Option[BucketType] = None,
    private[scala] val ejectionMethod: Option[EjectionMethod] = None,
    private[scala] val maxTTL: Option[Int] = None,
    private[scala] val compressionMode: Option[CompressionMode] = None,
    private[scala] val conflictResolutionType: Option[ConflictResolutionType] = None
) {
  def flushEnabled(value: Boolean): CreateBucketSettings = {
    copy(flushEnabled = Some(value))
  }

  def ramQuotaMB(value: Int): CreateBucketSettings = {
    copy(ramQuotaMB = value)
  }

  def numReplicas(value: Int): CreateBucketSettings = {
    copy(numReplicas = Some(value))
  }

  def replicaIndexes(value: Boolean): CreateBucketSettings = {
    copy(replicaIndexes = Some(value))
  }

  def bucketType(value: BucketType): CreateBucketSettings = {
    copy(bucketType = Some(value))
  }

  def ejectionMethod(value: EjectionMethod): CreateBucketSettings = {
    copy(ejectionMethod = Some(value))
  }

  def maxTTL(value: Int): CreateBucketSettings = {
    copy(maxTTL = Some(value))
  }

  def compressionMode(value: CompressionMode): CreateBucketSettings = {
    copy(compressionMode = Some(value))
  }

  def conflictResolutionType(value: ConflictResolutionType): CreateBucketSettings = {
    copy(conflictResolutionType = Some(value))
  }
}

object CreateBucketSettings {
  implicit val rw: CouchbasePickler.ReadWriter[CreateBucketSettings] = CouchbasePickler.macroRW
}

@Volatile
case class BucketSettings(
    name: String,
    @upickle.implicits.key("flush")
    flushEnabled: Boolean,
    @upickle.implicits.key("quota")
    ramQuotaMB: Int,
    @upickle.implicits.key("replicaNumber")
    numReplicas: Int,
    @upickle.implicits.key("replicaIndex")
    replicaIndexes: Boolean,
    bucketType: BucketType,
    @upickle.implicits.key("evictionPolicy")
    ejectionMethod: EjectionMethod,
    maxTTL: Int,
    compressionMode: CompressionMode,
    @Internal private[scala] val healthy: Boolean
) {
  def toCreateBucketSettings: CreateBucketSettings = {
    CreateBucketSettings(
      name,
      ramQuotaMB,
      Some(flushEnabled),
      Some(numReplicas),
      Some(replicaIndexes),
      Some(bucketType),
      Some(ejectionMethod),
      Some(maxTTL),
      Some(compressionMode)
    )
  }
}

object BucketSettings {
  implicit val rw: CouchbasePickler.ReadWriter[BucketSettings] = CouchbasePickler
    .readwriter[ujson.Obj]
    .bimap[BucketSettings](
      (x: BucketSettings) => {
        // Serialization not used
        ujson.Obj()
      },
      (json: ujson.Obj) => {
        val flushEnabled = Try(json("flush").bool).toOption.getOrElse(false)
        val rawRAM       = json("quota")("rawRAM").num.toInt
        val ramMB        = rawRAM / (1024 * 1024)
        val numReplicas  = json("replicaNumber").num.toInt
        val nodes        = json("nodes").arr
        val isHealthy    = nodes.nonEmpty && !nodes.exists(_.obj("status").str != "healthy")
        // Next two parameters only available post 5.X
        val maxTTL = json.value.get("maxTTL").map(_.num.toInt).getOrElse(0)
        val compressionMode = json.value
          .get("compressionMode")
          .map(v => CouchbasePickler.read[CompressionMode](v))
          .getOrElse(CompressionMode.Off)

        BucketSettings(
          json("name").str,
          flushEnabled,
          ramMB,
          numReplicas,
          Try(json("replicaIndex").bool).toOption.getOrElse(false),
          CouchbasePickler.read[BucketType](json("bucketType")),
          CouchbasePickler.read[EjectionMethod](json("evictionPolicy")),
          maxTTL,
          compressionMode,
          isHealthy
        )
      }
    )
}
