package com.couchbase.client.scala

import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.JsonObject
import reactor.core.scala.publisher.Mono

import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{ExecutionContext, Future}

class ReactiveCollection(val coll: Collection) {
  private val async = coll.async()
  private val kvTimeout = coll.kvTimeout

  def insert(doc: JsonDocument,
             timeout: FiniteDuration = kvTimeout,
             expiration: FiniteDuration = 0.seconds,
             replicateTo: ReplicateTo.Value = ReplicateTo.NONE,
             persistTo: PersistTo.Value = PersistTo.NONE
            )(implicit ec: ExecutionContext): Mono[JsonDocument] = {
    Mono.fromFuture(async.insert(doc, timeout, expiration, replicateTo, persistTo))
  }

  def insert(doc: JsonDocument,
             options: InsertOptions
            )(implicit ec: ExecutionContext): Mono[JsonDocument] = {
    Mono.fromFuture(async.insert(doc, options))
  }

  def insert(doc: JsonDocument,
             options: InsertOptionsBuilt
            )(implicit ec: ExecutionContext): Mono[JsonDocument] = {
    Mono.fromFuture(async.insert(doc, options))
  }

  def insertContent(id: String,
             content: JsonObject,
             timeout: FiniteDuration = kvTimeout,
             expiration: FiniteDuration = 0.seconds,
             replicateTo: ReplicateTo.Value = ReplicateTo.NONE,
             persistTo: PersistTo.Value = PersistTo.NONE
            )(implicit ec: ExecutionContext): Mono[JsonDocument] = {
    Mono.fromFuture(async.insertContent(id, content, timeout, expiration, replicateTo, persistTo))
  }

  def replace(doc: JsonDocument,
              timeout: FiniteDuration = kvTimeout,
              expiration: FiniteDuration = 0.seconds,
              replicateTo: ReplicateTo.Value = ReplicateTo.NONE,
              persistTo: PersistTo.Value = PersistTo.NONE
             )(implicit ec: ExecutionContext): Mono[JsonDocument] = {
    Mono.fromFuture(async.replace(doc, timeout, expiration, replicateTo, persistTo))
  }

  def replace(doc: JsonDocument,
              options: ReplaceOptions
             )(implicit ec: ExecutionContext): Mono[JsonDocument] = {
    Mono.fromFuture(async.replace(doc, options))
  }

  def replace(doc: JsonDocument,
              options: ReplaceOptionsBuilt
             )(implicit ec: ExecutionContext): Mono[JsonDocument] = {
    Mono.fromFuture(async.replace(doc, options))
  }

  def get(id: String,
          timeout: FiniteDuration = kvTimeout)
         (implicit ec: ExecutionContext): Mono[Option[JsonDocument]] = {
    Mono.fromFuture(async.get(id, timeout))
  }

  def get(id: String,
          options: GetOptions)
         (implicit ec: ExecutionContext): Mono[Option[JsonDocument]] = {
    Mono.fromFuture(async.get(id, options))
  }

  def get(id: String,
          options: GetOptionsBuilt)
         (implicit ec: ExecutionContext): Mono[Option[JsonDocument]] = {
    Mono.fromFuture(async.get(id, options))
  }

  def getOrError(id: String,
                 timeout: FiniteDuration = kvTimeout)
                (implicit ec: ExecutionContext): Mono[JsonDocument] = {
    Mono.fromFuture(async.getOrError(id, timeout))
  }

  def getOrError(id: String,
                 options: GetOptions)
                (implicit ec: ExecutionContext): Mono[JsonDocument] = {
    Mono.fromFuture(async.getOrError(id, options))
  }

  def getOrError(id: String,
                 options: GetOptionsBuilt)
                (implicit ec: ExecutionContext): Mono[JsonDocument] = {
    Mono.fromFuture(async.getOrError(id, options))
  }

  def getAndLock(id: String,
                 lockFor: FiniteDuration,
                 timeout: FiniteDuration = kvTimeout)
                (implicit ec: ExecutionContext): Mono[Option[JsonDocument]] = {
    Mono.fromFuture(async.getAndLock(id, lockFor, timeout))
  }

  def getAndLock(id: String,
                 lockFor: FiniteDuration,
                 options: GetAndLockOptions)
                (implicit ec: ExecutionContext): Mono[Option[JsonDocument]] = {
    Mono.fromFuture(async.getAndLock(id, lockFor, options))
  }

  def getAndLock(id: String,
                 lockFor: FiniteDuration,
                 options: GetAndLockOptionsBuilt)
                (implicit ec: ExecutionContext): Mono[Option[JsonDocument]] = {
    Mono.fromFuture(async.getAndLock(id, lockFor, options))
  }

}
