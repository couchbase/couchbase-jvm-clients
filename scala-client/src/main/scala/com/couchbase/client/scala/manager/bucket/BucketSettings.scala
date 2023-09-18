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
import com.couchbase.client.core.config
import com.couchbase.client.core.error.CouchbaseException
import com.couchbase.client.core.manager.bucket.{
  CoreBucketSettings,
  CoreCompressionMode,
  CoreConflictResolutionType,
  CoreCreateBucketSettings,
  CoreEvictionPolicyType,
  CoreStorageBackend
}
import com.couchbase.client.core.msg.kv.DurabilityLevel
import com.couchbase.client.scala.durability.Durability
import com.couchbase.client.scala.util.{CouchbasePickler, DurationConversions}

import java.lang
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration

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
    * Only supported for buckets of type [[BucketType.Couchbase]].
    */
  case object FullEviction extends EjectionMethod {
    override def alias: String = "fullEviction"
  }

  /** When ejecting an item, only eject the value (body), leaving the id and other metadata.
    *
    * Only supported for buckets of type [[BucketType.Couchbase]].
    */
  case object ValueOnly extends EjectionMethod {
    override def alias: String = "valueOnly"
  }

  /** Couchbase Server keeps all data until explicitly deleted, but will reject
    * any new data if you reach the quota (dedicated memory) you set for your bucket.
    *
    * Only supported for buckets of type [[BucketType.Ephemeral]].
    */
  case object NoEviction extends EjectionMethod {
    override def alias: String = "noEviction"
  }

  /** When the memory quota is reached, Couchbase Server ejects data that has
    * not been used recently.
    *
    * Only supported for buckets of type [[BucketType.Ephemeral]].
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

/** The type of conflict resolution to configure for the bucket.
  *
  * A conflict is caused when the source and target copies of an XDCR-replicated document are updated independently
  * of and dissimilarly to one another, each by a local application. The conflict must be resolved, by determining
  * which of the variants should prevail; and then correspondingly saving both documents in identical form. XDCR
  * provides an automated conflict resolution process.
  */
sealed trait ConflictResolutionType {
  def alias: String
}

object ConflictResolutionType {

  /** Conflict resolution based on a timestamp.
    *
    * Timestamp-based conflict resolution (often referred to as Last Write Wins, or LWW) uses the document
    * timestamp (stored in the CAS) to resolve conflicts. The timestamps associated with the most recent
    * updates of source and target documents are compared. The document whose update has the more recent
    * timestamp prevails.
    */
  case object Timestamp extends ConflictResolutionType {
    override def alias: String = "lww"
  }

  /** Conflict resolution based on the "Sequence Number".
    *
    * Conflicts can be resolved by referring to documents' sequence numbers. Sequence numbers are maintained
    * per document, and are incremented on every document-update. The sequence numbers of source and
    * target documents are compared; and the document with the higher sequence number prevails.
    */
  case object SequenceNumber extends ConflictResolutionType {
    override def alias: String = "seqno"
  }

  /** Custom bucket conflict resolution.
    *
    * In Couchbase Server 7.1, this feature is only available in "developer-preview" mode. See the UI XDCR settings
    * for the custom conflict resolution properties.
    */
  @Volatile
  case object Custom extends ConflictResolutionType {
    override def alias: String = "custom"
  }

  implicit val rw: CouchbasePickler.ReadWriter[ConflictResolutionType] = CouchbasePickler
    .readwriter[String]
    .bimap[ConflictResolutionType](
      x => x.alias,
      str =>
        str match {
          case "lww"    => Timestamp
          case "seqno"  => SequenceNumber
          case "custom" => Custom
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
    private[scala] val minimumDurabilityLevel: Option[Durability] = None,
    private[scala] val storageBackend: Option[StorageBackend] = None,
    private[scala] val historyRetentionCollectionDefault: Option[Boolean] = None,
    private[scala] val historyRetentionBytes: Option[Long] = None,
    private[scala] val historyRetentionDuration: Option[Duration] = None
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

  /** Specifies the storage backend to use for this bucket.
    *
    * @param value the storage backend the new bucket will use.
    * @return this, for chaining.
    */
  def storageBackend(value: StorageBackend): CreateBucketSettings = {
    copy(storageBackend = Some(value))
  }

  def historyRetentionCollectionDefault(value: Boolean): CreateBucketSettings = {
    copy(historyRetentionCollectionDefault = Some(value))
  }

  def historyRetentionBytes(value: Long): CreateBucketSettings = {
    copy(historyRetentionBytes = Some(value))
  }

  def historyRetentionDuration(value: Duration): CreateBucketSettings = {
    copy(historyRetentionDuration = Some(value))
  }

  private[scala] def toCore: CoreBucketSettings = {
    val x = this
    new CoreBucketSettings {
      override def name(): String = x.name

      override def flushEnabled(): lang.Boolean = x.flushEnabled.map(lang.Boolean.valueOf).orNull

      override def ramQuotaMB(): Long = x.ramQuotaMB

      override def numReplicas(): Integer = x.numReplicas.map(Integer.valueOf).orNull

      override def replicaIndexes(): lang.Boolean =
        x.replicaIndexes.map(lang.Boolean.valueOf).orNull

      override def bucketType(): config.BucketType =
        x.bucketType.map {
          case BucketType.Couchbase => config.BucketType.COUCHBASE
          case BucketType.Memcached => config.BucketType.MEMCACHED
          case BucketType.Ephemeral => config.BucketType.EPHEMERAL
        }.orNull

      override def evictionPolicy(): CoreEvictionPolicyType =
        x.ejectionMethod.map {
          case EjectionMethod.FullEviction    => CoreEvictionPolicyType.FULL
          case EjectionMethod.ValueOnly       => CoreEvictionPolicyType.VALUE_ONLY
          case EjectionMethod.NoEviction      => CoreEvictionPolicyType.NO_EVICTION
          case EjectionMethod.NotRecentlyUsed => CoreEvictionPolicyType.NOT_RECENTLY_USED
        }.orNull

      override def maxExpiry(): java.time.Duration =
        x.maxTTL.map(v => java.time.Duration.ofSeconds(v)).orNull

      override def compressionMode(): CoreCompressionMode =
        x.compressionMode.map {
          case CompressionMode.Off     => CoreCompressionMode.OFF
          case CompressionMode.Passive => CoreCompressionMode.PASSIVE
          case CompressionMode.Active  => CoreCompressionMode.ACTIVE
        }.orNull

      override def minimumDurabilityLevel(): DurabilityLevel =
        x.minimumDurabilityLevel.map {
          case Durability.Disabled             => DurabilityLevel.NONE
          case Durability.ClientVerified(_, _) => DurabilityLevel.NONE
          case Durability.Majority             => DurabilityLevel.MAJORITY
          case Durability.MajorityAndPersistToActive =>
            DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE
          case Durability.PersistToMajority => DurabilityLevel.PERSIST_TO_MAJORITY
        }.orNull

      override def storageBackend(): CoreStorageBackend =
        x.storageBackend.map {
          case StorageBackend.Couchstore => CoreStorageBackend.COUCHSTORE
          case StorageBackend.Magma      => CoreStorageBackend.MAGMA
        }.orNull

      override def historyRetentionCollectionDefault(): lang.Boolean =
        x.historyRetentionCollectionDefault.map(lang.Boolean.valueOf).orNull

      override def historyRetentionBytes(): lang.Long =
        x.historyRetentionBytes.map(lang.Long.valueOf).orNull

      override def historyRetentionDuration(): java.time.Duration =
        x.historyRetentionDuration.map(v => DurationConversions.scalaDurationToJava(v)).orNull
    }
  }

  private[scala] def toCoreCreateBucketSettings: CoreCreateBucketSettings = {
    val x = this
    new CoreCreateBucketSettings {
      override def conflictResolutionType(): CoreConflictResolutionType =
        x.conflictResolutionType match {
          case Some(ConflictResolutionType.Timestamp) => CoreConflictResolutionType.TIMESTAMP
          case Some(ConflictResolutionType.Custom)    => CoreConflictResolutionType.CUSTOM
          case Some(ConflictResolutionType.SequenceNumber) =>
            CoreConflictResolutionType.SEQUENCE_NUMBER
          case None => null
        }
    }
  }
}

@Volatile
case class BucketSettings(
    name: String,
    flushEnabled: Boolean,
    ramQuotaMB: Int,
    numReplicas: Int,
    replicaIndexes: Boolean,
    bucketType: BucketType,
    ejectionMethod: EjectionMethod,
    maxTTL: Option[Int],
    compressionMode: Option[CompressionMode],
    minimumDurabilityLevel: Durability,
    @Internal private[scala] val healthy: Boolean,
    storageBackend: Option[StorageBackend] = None,
    historyRetentionCollectionDefault: Option[Boolean] = None,
    historyRetentionBytes: Option[Long] = None,
    historyRetentionDuration: Option[Duration] = None
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
      maxTTL,
      compressionMode,
      None,
      Some(minimumDurabilityLevel),
      storageBackend,
      historyRetentionCollectionDefault,
      historyRetentionBytes,
      historyRetentionDuration
    )
  }
}

private[scala] object BucketSettings {
  def fromCore(core: CoreBucketSettings): BucketSettings = {
    val out = BucketSettings(
      core.name,
      core.flushEnabled,
      core.ramQuotaMB.toInt,
      core.numReplicas,
      core.replicaIndexes,
      core.bucketType match {
        case com.couchbase.client.core.config.BucketType.COUCHBASE => BucketType.Couchbase
        case com.couchbase.client.core.config.BucketType.EPHEMERAL => BucketType.Ephemeral
        case com.couchbase.client.core.config.BucketType.MEMCACHED => BucketType.Memcached
        case _                                                     => throw new CouchbaseException(s"Unknown bucket type ${core.bucketType}")
      },
      core.evictionPolicy match {
        case CoreEvictionPolicyType.FULL              => EjectionMethod.FullEviction
        case CoreEvictionPolicyType.VALUE_ONLY        => EjectionMethod.ValueOnly
        case CoreEvictionPolicyType.NOT_RECENTLY_USED => EjectionMethod.NotRecentlyUsed
        case CoreEvictionPolicyType.NO_EVICTION       => EjectionMethod.NoEviction
        case _                                        => throw new CouchbaseException(s"Unknown eviction type ${core.evictionPolicy}")
      },
      Option(core.maxExpiry).map(v => TimeUnit.NANOSECONDS.toSeconds(v.toNanos).toInt),
      Option(core.compressionMode).map {
        case CoreCompressionMode.OFF     => CompressionMode.Off
        case CoreCompressionMode.PASSIVE => CompressionMode.Passive
        case CoreCompressionMode.ACTIVE  => CompressionMode.Active
        case _                           => throw new CouchbaseException(s"Unknown compression type ${core.compressionMode}")
      },
      core.minimumDurabilityLevel match {
        case DurabilityLevel.NONE                           => Durability.Disabled
        case DurabilityLevel.MAJORITY                       => Durability.Majority
        case DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE => Durability.MajorityAndPersistToActive
        case DurabilityLevel.PERSIST_TO_MAJORITY            => Durability.PersistToMajority
        case _ =>
          throw new CouchbaseException(s"Unknown durability type ${core.minimumDurabilityLevel}")
      },
      false,
      // The server can return string "undefined"
      Option(core.storageBackend).filterNot(_.alias == "undefined").map {
        case CoreStorageBackend.COUCHSTORE => StorageBackend.Couchstore
        case CoreStorageBackend.MAGMA      => StorageBackend.Magma
        case _                             => throw new CouchbaseException(s"Unknown storage type ${core.storageBackend}")
      },
      // Cannot just do Option(x) here due to java.lang.Boolean and scala.Boolean
      if (core.historyRetentionCollectionDefault != null)
        Some(core.historyRetentionCollectionDefault)
      else None,
      if (core.historyRetentionBytes != null) Some(core.historyRetentionBytes) else None,
      if (core.historyRetentionDuration != null)
        Some(core.historyRetentionDuration).map(DurationConversions.javaDurationToScala)
      else None
    )
    out
  }
}

/** Specifies the underlying storage backend.
  */
sealed trait StorageBackend

object StorageBackend {
  case object Couchstore extends StorageBackend

  /** The Magma storage backend is an Enterprise Edition feature.
    */
  case object Magma extends StorageBackend
}
