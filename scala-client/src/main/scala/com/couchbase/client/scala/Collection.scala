package com.couchbase.client.scala

import scala.concurrent.{ExecutionContext, Future}
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

  def insert(doc: JsonDocument,
             options: InsertOptions,
            ): JsonDocument = null

  def insert(doc: JsonDocument,
             options: InsertOptionsBuilt,
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

  def replace(doc: JsonDocument,
             options: ReplaceOptions,
            ): JsonDocument = null

  def replace(doc: JsonDocument,
              options: ReplaceOptionsBuilt,
             ): JsonDocument = null

  def get(id: String,
          timeout: FiniteDuration = config.keyValueTimeout()): Option[JsonDocument] = Option.empty

  def get(id: String,
          options: GetOptionsBuilt): Option[JsonDocument] = Option.empty

  def get(id: String,
          options: GetOptions): Option[JsonDocument] = Option.empty

  def getOrError(id: String,
          timeout: FiniteDuration = config.keyValueTimeout()): JsonDocument = null

  def getOrError(id: String,
                 options: GetOptions): JsonDocument = null

  def getOrError(id: String,
                 options: GetOptionsBuilt): JsonDocument = null

  def getAndLock(id: String,
                 lockFor: FiniteDuration,
                 timeout: FiniteDuration = config.keyValueTimeout()): Option[JsonDocument] = Option.empty

  def getAndLock(id: String,
                 lockFor: FiniteDuration,
                 options: GetAndLockOptions): Option[JsonDocument] = Option.empty

  def getAndLock(id: String,
                 lockFor: FiniteDuration,
                 options: GetAndLockOptionsBuilt): Option[JsonDocument] = Option.empty

}
