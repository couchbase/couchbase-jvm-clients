package com.couchbase.client.scala

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._

class Collection(val config: CollectionConfig = CollectionConfig.default()) {
  // All methods are placeholders returning null for now
  def insert(doc: JsonDocument,
             timeout: FiniteDuration = config.keyValueTimeout(),
             expiration: FiniteDuration = 0.seconds,
             replicateTo: ReplicateTo.Value = ReplicateTo.NONE,
             persistTo: PersistTo.Value = PersistTo.NONE
            ): JsonDocument = null

  def insertContent(id: String,
             content: JsonObject,
             timeout: FiniteDuration = config.keyValueTimeout(),
             expiration: FiniteDuration = 0.seconds,
             replicateTo: ReplicateTo.Value = ReplicateTo.NONE,
             persistTo: PersistTo.Value = PersistTo.NONE
            ): JsonDocument = null

  def replace(doc: JsonDocument,
              timeout: FiniteDuration = config.keyValueTimeout(),
              expiration: FiniteDuration = 0.seconds,
              replicateTo: ReplicateTo.Value = ReplicateTo.NONE,
              persistTo: PersistTo.Value = PersistTo.NONE
             ): JsonDocument = null

  def get(id: String,
          timeout: FiniteDuration = config.keyValueTimeout()): Option[JsonDocument] = Option.empty

  def getAndLock(id: String,
                 lockFor: FiniteDuration,
                 timeout: FiniteDuration = config.keyValueTimeout()): Option[JsonDocument] = Option.empty

}
