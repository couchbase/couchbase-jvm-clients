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

  def get(id: String,
          timeout: FiniteDuration = config.keyValueTimeout())
         (implicit ec: ExecutionContext): Future[Option[JsonDocument]] = Future { Option.empty }

  def getOrError(id: String,
                 timeout: FiniteDuration = config.keyValueTimeout()): Future[JsonDocument] = null

  def getAndLock(id: String,
                 lockFor: FiniteDuration,
                 timeout: FiniteDuration = config.keyValueTimeout())
                (implicit ec: ExecutionContext): Future[Option[JsonDocument]] = Future { Option.empty }

}
