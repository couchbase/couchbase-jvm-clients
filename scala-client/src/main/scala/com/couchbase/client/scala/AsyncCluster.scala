package com.couchbase.client.scala

import java.time.Duration
import java.util.concurrent.TimeUnit

import com.couchbase.client.core.Core
import com.couchbase.client.core.msg.query.QueryRequest
import com.couchbase.client.scala.api.QueryOptions
import com.couchbase.client.scala.env.ClusterEnvironment
import com.couchbase.client.scala.query.{QueryConsumer, QueryResult}
import com.couchbase.client.scala.util.Conversions
import io.netty.util.CharsetUtil

import scala.compat.java8.FutureConverters
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

class AsyncCluster(environment: => ClusterEnvironment)
                  (implicit ec: ExecutionContext) {
  private val core = Core.create(environment)

  // TODO MVP other credentials
  // TODO MVP query
  // TODO MVP shutdown

  implicit def scalaFiniteDurationToJava(in: scala.concurrent.duration.FiniteDuration): java.time.Duration = {
    java.time.Duration.ofNanos(in.toNanos)
  }

  implicit def scalaDurationToJava(in: scala.concurrent.duration.Duration): java.time.Duration = {
    java.time.Duration.ofNanos(in.toNanos)
  }

  implicit def javaDurationToScala(in: java.time.Duration): scala.concurrent.duration.FiniteDuration = {
    FiniteDuration.apply(in.toNanos, TimeUnit.NANOSECONDS)
  }

  def bucket(name: String): Future[AsyncBucket] = {
    Conversions.monoToFuture(core.openBucket(name))
      .map(v => new AsyncBucket(name, core, environment))
  }

  def query(statement: String, options: QueryOptions): Future[QueryResult] = {
    val result = new QueryConsumer()

    // TODO: proper jackson encoding with options
    val query = ("{\"statement\":\"" + statement + "\"}").getBytes(CharsetUtil.UTF_8)

    val timeout: java.time.Duration = options.timeout match {
      case Some(v) => v
      case _ => environment.queryTimeout()
    }

    val retryStrategy = options.retryStrategy match {
      case Some(v) => v
      case _ => environment.retryStrategy()
    }

    val request = new QueryRequest(timeout,
      core.context(),
      retryStrategy,
      environment.credentials(),
      query,
      result)

    core.send(request)

    FutureConverters.toScala(request.response())
      .map(response => {
        new QueryResult(result)
      })
  }
}

//object AsyncCluster {
////  def connect(username: String, password: String) = {
////    AsyncCluster(() => ClusterEnvironment.create(usernmae, password))
////  }
//}