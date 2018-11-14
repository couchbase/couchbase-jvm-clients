package com.couchbase.client.scala

import java.util.concurrent.CompletableFuture

import com.couchbase.client.core.{Core, CoreContext}
import com.couchbase.client.core.annotation.Stability
import com.couchbase.client.core.msg.{Request, Response}
import com.couchbase.client.core.msg.kv.GetRequest

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.{FiniteDuration, _}




class AsyncCollection(val collection: Collection) {
  private val config = collection.scope.cluster.env
  private val core: Core = null
  private var coreContext: CoreContext = null

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

  def getOrError(id: String,
                 options: GetOptions): Future[JsonDocument] = null

  def getOrError(id: String,
                 options: GetOptionsBuilt): Future[JsonDocument] = null

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

//  @Stability.Internal
//  def get[T](id: String, request: GetRequest): Future[JsonDocument] = {
//    dispatch(request)
//    request.response.thenApply((getResponse: GetResponse) => {
//      def foo(getResponse: GetResponse) = { // todo: implement decoding and response code checking
//        new Document[T](id, null, getResponse.cas)
//      }
//
//      foo(getResponse)
//    })
//  }

  private def dispatch(request: Request[_ <: Response]): Unit = {
    core.send(request)
  }

}
