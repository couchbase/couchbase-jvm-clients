package com.couchbase.client.scala

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._

class Collection(val name: String,
                 val scope: Scope) {
  private val config = scope.cluster.env
  private val asyncColl = new AsyncCollection(this)
  private val reactiveColl = new ReactiveCollection(this)
  private val safetyTimeout = 60.minutes

  // All methods are placeholders returning null for now
  def insert(doc: JsonDocument,
             timeout: FiniteDuration = config.keyValueTimeout(),
             expiration: FiniteDuration = 0.seconds,
             replicateTo: ReplicateTo.Value = ReplicateTo.NONE,
             persistTo: PersistTo.Value = PersistTo.NONE
            )(implicit ec: ExecutionContext): JsonDocument = {
    Await.result(asyncColl.insert(doc, timeout, expiration, replicateTo, persistTo), safetyTimeout)
  }

  def insert(doc: JsonDocument,
             options: InsertOptions,
            )(implicit ec: ExecutionContext): JsonDocument = {
    Await.result(asyncColl.insert(doc, options), safetyTimeout)
  }

  def insert(doc: JsonDocument,
             options: InsertOptionsBuilt,
            )(implicit ec: ExecutionContext): JsonDocument = {
    Await.result(asyncColl.insert(doc, options), safetyTimeout)
  }

  def insertContent(id: String,
             content: JsonObject,
             timeout: FiniteDuration = config.keyValueTimeout(),
             expiration: FiniteDuration = 0.seconds,
             replicateTo: ReplicateTo.Value = ReplicateTo.NONE,
             persistTo: PersistTo.Value = PersistTo.NONE
            )(implicit ec: ExecutionContext): JsonDocument = {
    Await.result(asyncColl.insertContent(id, content, timeout, expiration, replicateTo, persistTo), safetyTimeout)
  }

  def replace(doc: JsonDocument,
              timeout: FiniteDuration = config.keyValueTimeout(),
              expiration: FiniteDuration = 0.seconds,
              replicateTo: ReplicateTo.Value = ReplicateTo.NONE,
              persistTo: PersistTo.Value = PersistTo.NONE
             )(implicit ec: ExecutionContext): JsonDocument = {
    Await.result(asyncColl.replace(doc, timeout, expiration, replicateTo, persistTo), safetyTimeout)
  }

  def replace(doc: JsonDocument,
             options: ReplaceOptions,
            )(implicit ec: ExecutionContext): JsonDocument = {
    Await.result(asyncColl.replace(doc, options), safetyTimeout)
  }

  def replace(doc: JsonDocument,
              options: ReplaceOptionsBuilt,
             )(implicit ec: ExecutionContext): JsonDocument = {
    Await.result(asyncColl.replace(doc, options), safetyTimeout)
  }

  def get(id: String,
          timeout: FiniteDuration = config.keyValueTimeout())
         (implicit ec: ExecutionContext): Option[JsonDocument] = {
    Await.result(asyncColl.get(id, timeout), safetyTimeout)
  }

  def get(id: String,
          options: GetOptionsBuilt)
         (implicit ec: ExecutionContext): Option[JsonDocument] = {
    Await.result(asyncColl.get(id, options), safetyTimeout)
  }

  def get(id: String,
          options: GetOptions)
         (implicit ec: ExecutionContext): Option[JsonDocument] = {
    Await.result(asyncColl.get(id, options), safetyTimeout)
  }

  def getOrError(id: String,
          timeout: FiniteDuration = config.keyValueTimeout())
                (implicit ec: ExecutionContext): JsonDocument = {
    Await.result(asyncColl.getOrError(id, timeout), safetyTimeout)
  }

  def getOrError(id: String,
                 options: GetOptions)
                (implicit ec: ExecutionContext): JsonDocument = {
    Await.result(asyncColl.getOrError(id, options), safetyTimeout)
  }

  def getOrError(id: String,
                 options: GetOptionsBuilt)
                (implicit ec: ExecutionContext): JsonDocument = {
    Await.result(asyncColl.getOrError(id, options), safetyTimeout)
  }

  def getAndLock(id: String,
                 lockFor: FiniteDuration,
                 timeout: FiniteDuration = config.keyValueTimeout())
                (implicit ec: ExecutionContext): Option[JsonDocument] = {
    Await.result(asyncColl.getAndLock(id, lockFor, timeout), safetyTimeout)
  }

  def getAndLock(id: String,
                 lockFor: FiniteDuration,
                 options: GetAndLockOptions)
                (implicit ec: ExecutionContext): Option[JsonDocument] = {
    Await.result(asyncColl.getAndLock(id, lockFor, options), safetyTimeout)
  }

  def getAndLock(id: String,
                 lockFor: FiniteDuration,
                 options: GetAndLockOptionsBuilt)
                (implicit ec: ExecutionContext): Option[JsonDocument] = {
    Await.result(asyncColl.getAndLock(id, lockFor, options), safetyTimeout)
  }

  def async(): AsyncCollection = asyncColl
  def reactive(): ReactiveCollection = reactiveColl
}
