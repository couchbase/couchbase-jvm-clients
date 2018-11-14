package com.couchbase.client.scala

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.{FiniteDuration, _}




class AsyncCollection(val config: CollectionConfig = CollectionConfig.default()) {
  // All methods are placeholders returning null for now
  def insert(doc: JsonDocument,
             timeout: FiniteDuration = config.keyValueTimeout(),
             expiration: FiniteDuration = 0.seconds,
             replicateTo: ReplicateTo.Value = ReplicateTo.NONE,
             persistTo: PersistTo.Value = PersistTo.NONE
            )(implicit ec: ExecutionContext): Future[JsonDocument] = null

  def insert(doc: JsonDocument,
             options: InsertOptions
            )(implicit ec: ExecutionContext): Future[JsonDocument] = null

  def insert(doc: JsonDocument,
             options: InsertOptionsBuilt
            )(implicit ec: ExecutionContext): Future[JsonDocument] = null

  def insertContent(id: String,
             content: JsonObject,
             timeout: FiniteDuration = config.keyValueTimeout(),
             expiration: FiniteDuration = 0.seconds,
             replicateTo: ReplicateTo.Value = ReplicateTo.NONE,
             persistTo: PersistTo.Value = PersistTo.NONE
            )(implicit ec: ExecutionContext): Future[JsonDocument] = null

  def replace(doc: JsonDocument,
              timeout: FiniteDuration = config.keyValueTimeout(),
              expiration: FiniteDuration = 0.seconds,
              replicateTo: ReplicateTo.Value = ReplicateTo.NONE,
              persistTo: PersistTo.Value = PersistTo.NONE
             )(implicit ec: ExecutionContext): Future[JsonDocument] = null

  def replace(doc: JsonDocument,
              options: ReplaceOptions
             )(implicit ec: ExecutionContext): Future[JsonDocument] = null

  def replace(doc: JsonDocument,
              options: ReplaceOptionsBuilt
             )(implicit ec: ExecutionContext): Future[JsonDocument] = null

  def get(id: String,
          timeout: FiniteDuration = config.keyValueTimeout())
         (implicit ec: ExecutionContext): Future[Option[JsonDocument]] = Future { Option.empty }

  def get(id: String,
          options: GetOptions
         )(implicit ec: ExecutionContext): Future[Option[JsonDocument]] = Future { Option.empty }

  def get(id: String,
          options: GetOptionsBuilt
         )(implicit ec: ExecutionContext): Future[Option[JsonDocument]] = Future { Option.empty }

  def getOrError(id: String,
                 timeout: FiniteDuration = config.keyValueTimeout()): Future[JsonDocument] = null

  def getAndLock(id: String,
                 lockFor: FiniteDuration,
                 timeout: FiniteDuration = config.keyValueTimeout())
                (implicit ec: ExecutionContext): Future[Option[JsonDocument]] = Future { Option.empty }

  def getAndLock(id: String,
                 lockFor: FiniteDuration,
                 options: GetAndLockOptions)
                (implicit ec: ExecutionContext): Future[Option[JsonDocument]] = Future { Option.empty }

  def getAndLock(id: String,
                 lockFor: FiniteDuration,
                 options: GetAndLockOptionsBuilt)
                (implicit ec: ExecutionContext): Future[Option[JsonDocument]] = Future { Option.empty }
}
