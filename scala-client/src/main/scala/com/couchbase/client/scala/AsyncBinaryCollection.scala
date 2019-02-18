package com.couchbase.client.scala

import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.api.{CounterResult, MutationResult}
import com.couchbase.client.scala.durability.{Durability}
import com.couchbase.client.scala.durability.Durability._
import com.couchbase.client.scala.kv._
import io.opentracing.Span

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.concurrent.duration.{Duration, _}

class AsyncBinaryCollection(private[scala] val async: AsyncCollection)
                           (implicit ec: ExecutionContext) {
  import DurationConversions._

  private[scala] val environment = async.environment
  private[scala] val kvTimeout = async.kvTimeout
  private[scala] val binaryAppendHandler = new BinaryAppendHandler(async.hp)
  private[scala] val binaryPrependHandler = new BinaryPrependHandler(async.hp)
  private[scala] val binaryIncrementHandler = new BinaryIncrementHandler(async.hp)
  private[scala] val binaryDecrementHandler = new BinaryDecrementHandler(async.hp)

  def append(id: String,
             content: Array[Byte],
             cas: Long = 0,
             durability: Durability = Disabled,
             parentSpan: Option[Span] = None,
             timeout: Duration = kvTimeout,
             retryStrategy: RetryStrategy = environment.retryStrategy()): Future[MutationResult] = {
    val req = binaryAppendHandler.request(id, content, cas, durability, parentSpan, timeout, retryStrategy)
    async.wrapWithDurability(req, id, binaryAppendHandler, durability, false, timeout)
  }

  def prepend(id: String,
              content: Array[Byte],
              cas: Long = 0,
              durability: Durability = Disabled,
              parentSpan: Option[Span] = None,
              timeout: Duration = kvTimeout,
              retryStrategy: RetryStrategy = environment.retryStrategy()): Future[MutationResult] = {
    val req = binaryPrependHandler.request(id, content, cas, durability, parentSpan, timeout, retryStrategy)
    async.wrapWithDurability(req, id, binaryPrependHandler, durability, false, timeout)
  }

  def increment(id: String,
                delta: Long,
                initial: Option[Long] = None,
                cas: Long = 0,
                durability: Durability = Disabled,
                expiration: Duration = 0.seconds,
                parentSpan: Option[Span] = None,
                timeout: Duration = kvTimeout,
                retryStrategy: RetryStrategy = environment.retryStrategy()): Future[CounterResult] = {
    val req = binaryIncrementHandler.request(id, delta, initial, cas, durability, expiration, parentSpan, timeout, retryStrategy)
    async.wrapWithDurability(req, id, binaryIncrementHandler, durability, false, timeout)
  }

  def decrement(id: String,
                delta: Long,
                initial: Option[Long] = None,
                cas: Long = 0,
                durability: Durability = Disabled,
                expiration: Duration = 0.seconds,
                parentSpan: Option[Span] = None,
                timeout: Duration = kvTimeout,
                retryStrategy: RetryStrategy = environment.retryStrategy()): Future[CounterResult] = {
    val req = binaryDecrementHandler.request(id, delta, initial, cas, durability, expiration, parentSpan, timeout, retryStrategy)
    async.wrapWithDurability(req, id, binaryDecrementHandler, durability, false, timeout)
  }
}
