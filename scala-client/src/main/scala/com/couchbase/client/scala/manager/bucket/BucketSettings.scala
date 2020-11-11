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

import java.nio.charset.StandardCharsets

import com.couchbase.client.core.annotation.Stability.{Internal, Volatile}
import com.couchbase.client.scala.durability.Durability
import com.couchbase.client.scala.durability.Durability.{
  Disabled,
  Majority,
  MajorityAndPersistToActive,
  PersistToMajority
}
import com.couchbase.client.scala.json.{JsonArray, JsonObject}
import com.couchbase.client.scala.manager.bucket.BucketType.{Couchbase, Ephemeral, Memcached}
import com.couchbase.client.scala.manager.bucket.ConflictResolutionType.{SequenceNumber, Timestamp}
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

  /** When ejecting an item, eject all data related to it including the id.
    *
    * Only supported for buckets of type [[Couchbase]].
    */
  case object FullEviction extends EjectionMethod {
    override def alias: String = "fullEviction"
  }

  /** When ejecting an item, only eject the value (body), leaving the id and other metadata.
    *
    * Only supported for buckets of type [[Couchbase]].
    */
  case object ValueOnly extends EjectionMethod {
    override def alias: String = "valueOnly"
  }

  /** Couchbase Server keeps all data until explicitly deleted, but will reject
    * any new data if you reach the quota (dedicated memory) you set for your bucket.
    *
    * Only supported for buckets of type [[Ephemeral]].
    */
  case object NoEviction extends EjectionMethod {
    override def alias: String = "noEviction"
  }

  /** When the memory quota is reached, Couchbase Server ejects data that has
    * not been used recently.
    *
    * Only supported for buckets of type [[Ephemeral]].
    */
  case object NotRecentlyUsed extends EjectionMethod {
    override def alias: String = "nruEviction"
  }

  implicit val rw: CouchbasePickler.ReadWriter[EjectionMethod] = CouchbasePickler
    .readwriter[String]
    .bimap[EjectionMethod](
      x => x.alias,
      str =>
        str match {
          case "fullEviction" => FullEviction
          case "valueOnly"    => ValueOnly
          case "noEviction"   => NoEviction
          case "nruEviction"  => NotRecentlyUsed
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
    private[scala] val conflictResolutionType: Option[ConflictResolutionType] = None,
    private[scala] val minimumDurabilityLevel: Option[Durability] = None
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

  def minimumDurabilityLevel(value: Durability): CreateBucketSettings = {
    copy(minimumDurabilityLevel = Some(value))
  }
}

object CreateBucketSettings {
  implicit val rw: CouchbasePickler.ReadWriter[CreateBucketSettings] = CouchbasePickler.macroRW

  implicit val rwd: CouchbasePickler.ReadWriter[Durability] = BucketSettings.rw
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
    @upickle.implicits.key("durabilityMinLevel")
    minimumDurabilityLevel: Durability,
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
      Some(compressionMode),
      minimumDurabilityLevel = Some(minimumDurabilityLevel)
    )
  }
}

object BucketSettings {
  def parseFrom(raw: Array[Byte]): BucketSettings = {
    val json = JsonObject.fromJson(new String(raw, StandardCharsets.UTF_8))
    parseFrom(json)
  }

  def parseFrom(json: JsonObject): BucketSettings = {
    val flushEnabled = Try(json.bool("flush")).toOption.getOrElse(false)
    val rawRAM       = json.obj("quota").num("rawRAM")
    val ramMB        = rawRAM / (1024 * 1024)
    val numReplicas  = json.num("replicaNumber")
    val nodes        = json.arr("nodes")
    var isHealthy    = nodes.nonEmpty
    import scala.collection.JavaConverters._
    for (v <- nodes.values.asScala) {
      val j = v.asInstanceOf[JsonObject]
      if (j.str("status") != "healthy") {
        isHealthy = false
      }
    }
    // Next two parameters only available post 5.X
    val maxTTL = Try(json.num("maxTTL")).toOption.getOrElse(0)
    val compressionMode = Try('"' + json.str("compressionMode") + '"')
      .map(v => CouchbasePickler.read[CompressionMode](v))
      .getOrElse(CompressionMode.Off)

    val minimumDurabilityLevel = Try('"' + json.str("durabilityMinLevel") + '"')
      .map(v => CouchbasePickler.read[Durability](v))
      .getOrElse(Durability.Disabled)

    BucketSettings(
      json.str("name"),
      flushEnabled,
      ramMB,
      numReplicas,
      Try(json.bool("replicaIndex")).toOption.getOrElse(false),
      CouchbasePickler.read[BucketType]('"' + json.str("bucketType") + '"'),
      CouchbasePickler.read[EjectionMethod]('"' + json.str("evictionPolicy") + '"'),
      maxTTL,
      compressionMode,
      minimumDurabilityLevel,
      isHealthy
    )
  }

  def parseSeqFrom(raw: Array[Byte]): Seq[BucketSettings] = {
    val jsonArr = JsonArray.fromJson(new String(raw, StandardCharsets.UTF_8)).get
    import scala.collection.JavaConverters._
    jsonArr.values.asScala.map(v => {
      val j = v.asInstanceOf[JsonObject]
      parseFrom(j)
    })
  }

  implicit val rw: CouchbasePickler.ReadWriter[Durability] = CouchbasePickler
    .readwriter[String]
    .bimap[Durability](
      {
        case Disabled                   => "none"
        case Majority                   => "majority"
        case MajorityAndPersistToActive => "majorityAndPersistActive"
        case PersistToMajority          => "persistToMajority"
      }, {
        case "none"                     => Disabled
        case "majority"                 => Majority
        case "majorityAndPersistActive" => MajorityAndPersistToActive
        case "persistToMajority"        => PersistToMajority
      }
    )
}
