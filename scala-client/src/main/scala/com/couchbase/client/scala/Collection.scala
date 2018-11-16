package com.couchbase.client.scala

import java.util.concurrent.TimeUnit

import com.couchbase.client.core.CouchbaseException
import com.couchbase.client.core.message.ResponseStatus
import com.couchbase.client.core.message.kv.{RemoveRequest, RemoveResponse}
import com.couchbase.client.java.bucket.api.Utils.addDetails
import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.env.CouchbaseEnvironment
import com.couchbase.client.java.error.{CASMismatchException, CouchbaseOutOfMemoryException, DocumentDoesNotExistException, TemporaryFailureException}

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._

class Collection(val name: String,
                 val scope: Scope) {
  private val config: CouchbaseEnvironment = null // scope.cluster.env
  private val asyncColl = new AsyncCollection(this)
//  private val reactiveColl = new ReactiveCollection(this)
  private val safetyTimeout = 60.minutes
//  val kvTimeout = FiniteDuration(config.kvTimeout(), TimeUnit.MILLISECONDS)
  val kvTimeout = FiniteDuration(2500, TimeUnit.MILLISECONDS)

  // All methods are placeholders returning null for now
  def insert(id: String,
             content: JsonObject,
             timeout: FiniteDuration = kvTimeout,
             expiration: FiniteDuration = 0.seconds,
             replicateTo: ReplicateTo.Value = ReplicateTo.NONE,
             persistTo: PersistTo.Value = PersistTo.NONE
            )(implicit ec: ExecutionContext): JsonDocument = {
    Await.result(asyncColl.insert(id, content, timeout, expiration, replicateTo, persistTo), safetyTimeout)
  }

  def insert(id: String,
             content: JsonObject,
             options: InsertOptions,
            )(implicit ec: ExecutionContext): JsonDocument = {
    Await.result(asyncColl.insert(id, content, options), safetyTimeout)
  }

  def replace(id: String,
              content: JsonObject,
              cas: Long,
              timeout: FiniteDuration = kvTimeout,
              expiration: FiniteDuration = 0.seconds,
              replicateTo: ReplicateTo.Value = ReplicateTo.NONE,
              persistTo: PersistTo.Value = PersistTo.NONE
             )(implicit ec: ExecutionContext): JsonDocument = {
    Await.result(asyncColl.replace(id, content, cas, timeout, expiration, replicateTo, persistTo), safetyTimeout)
  }

  def replace(id: String,
              content: JsonObject,
              cas: Long,
              options: ReplaceOptions,
            )(implicit ec: ExecutionContext): JsonDocument = {
    Await.result(asyncColl.replace(id, content, cas, options), safetyTimeout)
  }

  def remove(id: String,
             cas: Long,
             timeout: FiniteDuration = kvTimeout,
             replicateTo: ReplicateTo.Value = ReplicateTo.NONE,
             persistTo: PersistTo.Value = PersistTo.NONE
            )(implicit ec: ExecutionContext): RemoveResult = {
    Await.result(asyncColl.remove(id, cas, timeout, replicateTo, persistTo), safetyTimeout)
  }

  def remove(id: String,
             cas: Long,
             options: RemoveOptions
            )(implicit ec: ExecutionContext): RemoveResult = {
    Await.result(asyncColl.remove(id, cas, options), safetyTimeout)
  }

  def getFields(id: String,
                operations: GetFields,
                timeout: FiniteDuration = kvTimeout)
                    (implicit ec: ExecutionContext): FieldsResult = {
    null
  }

  def getFieldsAs[T](id: String,
                     operations: GetFields,
                     timeout: FiniteDuration = kvTimeout)
                    (implicit ec: ExecutionContext): T = {
    return null.asInstanceOf[T]
  }

  def getAs[T](id: String,
               timeout: FiniteDuration = kvTimeout)
              (implicit ec: ExecutionContext): Option[T] = null

  def getAs[T](id: String,
               options: GetOptions)
              (implicit ec: ExecutionContext): Option[T] = null

  def get(id: String,
          timeout: FiniteDuration = kvTimeout)
         (implicit ec: ExecutionContext): Option[JsonDocument] = {
    Await.result(asyncColl.get(id, timeout), safetyTimeout)
  }

  def get(id: String,
          options: GetOptions)
         (implicit ec: ExecutionContext): Option[JsonDocument] = {
    Await.result(asyncColl.get(id, options), safetyTimeout)
  }

  def getOrError(id: String,
          timeout: FiniteDuration = kvTimeout)
                (implicit ec: ExecutionContext): JsonDocument = {
    Await.result(asyncColl.getOrError(id, timeout), safetyTimeout)
  }

  def getOrError(id: String,
                 options: GetOptions)
                (implicit ec: ExecutionContext): JsonDocument = {
    Await.result(asyncColl.getOrError(id, options), safetyTimeout)
  }

  def getAndLock(id: String,
                 lockFor: FiniteDuration,
                 timeout: FiniteDuration = kvTimeout)
                (implicit ec: ExecutionContext): Option[JsonDocument] = {
    Await.result(asyncColl.getAndLock(id, lockFor, timeout), safetyTimeout)
  }

  def getAndLock(id: String,
                 lockFor: FiniteDuration,
                 options: GetAndLockOptions)
                (implicit ec: ExecutionContext): Option[JsonDocument] = {
    Await.result(asyncColl.getAndLock(id, lockFor, options), safetyTimeout)
  }

  def async(): AsyncCollection = asyncColl
//  def reactive(): ReactiveCollection = reactiveColl
}
