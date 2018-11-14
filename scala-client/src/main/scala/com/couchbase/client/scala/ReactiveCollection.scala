package com.couchbase.client.scala

import reactor.core.scala.publisher.Mono

import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{ExecutionContext, Future}

class ReactiveCollection(val config: CollectionConfig = CollectionConfig.default()) {
  // All methods are placeholders returning null for now
  def insert(doc: JsonDocument,
             timeout: FiniteDuration = config.keyValueTimeout(),
             expiration: FiniteDuration = 0.seconds,
             replicateTo: ReplicateTo.Value = ReplicateTo.NONE,
             persistTo: PersistTo.Value = PersistTo.NONE
            ): Mono[JsonDocument] = null

  def insert(doc: JsonDocument,
             options: InsertOptions
            ): Mono[JsonDocument] = null

  def insert(doc: JsonDocument,
             options: InsertOptionsBuilt
            ): Mono[JsonDocument] = null

  def insertContent(id: String,
             content: JsonObject,
             timeout: FiniteDuration = config.keyValueTimeout(),
             expiration: FiniteDuration = 0.seconds,
             replicateTo: ReplicateTo.Value = ReplicateTo.NONE,
             persistTo: PersistTo.Value = PersistTo.NONE
            ): Mono[JsonDocument] = null

  def replace(doc: JsonDocument,
              timeout: FiniteDuration = config.keyValueTimeout(),
              expiration: FiniteDuration = 0.seconds,
              replicateTo: ReplicateTo.Value = ReplicateTo.NONE,
              persistTo: PersistTo.Value = PersistTo.NONE
             ): Mono[JsonDocument] = null

  def replace(doc: JsonDocument,
              options: ReplaceOptions
             ): Mono[JsonDocument] = null

  def replace(doc: JsonDocument,
              options: ReplaceOptionsBuilt
             ): Mono[JsonDocument] = null

  def get(id: String,
          timeout: FiniteDuration = config.keyValueTimeout()): Mono[Option[JsonDocument]] = null

  def get(id: String,
          options: GetOptions): Mono[Option[JsonDocument]] = null

  def get(id: String,
          options: GetOptionsBuilt): Mono[Option[JsonDocument]] = null

  def getOrError(id: String,
                 timeout: FiniteDuration = config.keyValueTimeout()): Mono[JsonDocument] = null

  def getOrError(id: String,
                 options: GetOptions): Mono[JsonDocument] = null

  def getOrError(id: String,
                 options: GetOptionsBuilt): Mono[JsonDocument] = null

  def getAndLock(id: String,
                 lockFor: FiniteDuration,
                 timeout: FiniteDuration = config.keyValueTimeout()): Mono[Option[JsonDocument]] = null

  def getAndLock(id: String,
                 lockFor: FiniteDuration,
                 options: GetAndLockOptions): Mono[Option[JsonDocument]] = null

  def getAndLock(id: String,
                 lockFor: FiniteDuration,
                 options: GetAndLockOptionsBuilt): Mono[Option[JsonDocument]] = null

}
