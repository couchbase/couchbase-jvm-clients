package com.couchbase.client.scala

import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.api.{CounterResult, MutationResult}
import com.couchbase.client.scala.durability.{Disabled, Durability}
import io.opentracing.Span

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.concurrent.duration.{Duration, _}
import scala.util.Try

class BinaryCollection(val async: AsyncBinaryCollection)
                      (implicit ec: ExecutionContext) {
  private val kvTimeout = async.kvTimeout
  private val environment = async.environment
  val reactive = new ReactiveBinaryCollection(async)

  def append(id: String,
             content: Array[Byte],
             cas: Long = 0,
             durability: Durability = Disabled,
             parentSpan: Option[Span] = None,
             timeout: Duration = kvTimeout,
             retryStrategy: RetryStrategy = environment.retryStrategy()): Try[MutationResult] = {
    Collection.block(async.append(id, content, cas, durability, parentSpan, timeout, retryStrategy), timeout)

  }

  def prepend(id: String,
              content: Array[Byte],
              cas: Long = 0,
              durability: Durability = Disabled,
              parentSpan: Option[Span] = None,
              timeout: Duration = kvTimeout,
              retryStrategy: RetryStrategy = environment.retryStrategy()): Try[MutationResult] = {
    Collection.block(async.prepend(id, content, cas, durability, parentSpan, timeout, retryStrategy), timeout)
  }

  def increment(id: String,
                delta: Long,
                initial: Option[Long] = None,
                cas: Long = 0,
                durability: Durability = Disabled,
                expiration: Duration = 0.seconds,
                parentSpan: Option[Span] = None,
                timeout: Duration = kvTimeout,
                retryStrategy: RetryStrategy = environment.retryStrategy()): Try[CounterResult] = {
    Collection.block(async.increment(id, delta, initial, cas, durability, expiration, parentSpan, timeout, retryStrategy), timeout)
  }

  def decrement(id: String,
                delta: Long,
                initial: Option[Long] = None,
                cas: Long = 0,
                durability: Durability = Disabled,
                expiration: Duration = 0.seconds,
                parentSpan: Option[Span] = None,
                timeout: Duration = kvTimeout,
                retryStrategy: RetryStrategy = environment.retryStrategy()): Try[CounterResult] = {
    Collection.block(async.decrement(id, delta, initial, cas, durability, expiration, parentSpan, timeout, retryStrategy), timeout)
  }

}
