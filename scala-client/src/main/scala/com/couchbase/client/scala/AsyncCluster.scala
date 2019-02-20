package com.couchbase.client.scala

import java.time.Duration
import java.util.concurrent.TimeUnit

import com.couchbase.client.core.Core
import com.couchbase.client.core.env.Credentials
import com.couchbase.client.core.msg.kv.ObserveViaCasRequest
import com.couchbase.client.core.msg.query.QueryRequest
import com.couchbase.client.scala.api.QueryOptions
import com.couchbase.client.scala.env.ClusterEnvironment
import com.couchbase.client.scala.query.{QueryConsumer, QueryResult}
import com.couchbase.client.scala.util.AsyncUtils.DefaultTimeout
import com.couchbase.client.scala.util.{FutureConversions, Validate}
import io.netty.util.CharsetUtil
import reactor.core.scala.publisher.Mono

import scala.compat.java8.FutureConverters
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object DurationConversions {
  implicit def scalaDurationToJava(in: scala.concurrent.duration.Duration): java.time.Duration = {
    java.time.Duration.ofNanos(in.toNanos)
  }

  implicit def javaDurationToScala(in: java.time.Duration): scala.concurrent.duration.Duration = {
    scala.concurrent.duration.Duration(in.toNanos, TimeUnit.NANOSECONDS)
  }

}

class AsyncCluster(environment: => ClusterEnvironment)
                  (implicit ec: ExecutionContext) {
  private val core = Core.create(environment)
  private[scala] val env = environment

  // Opening resources will not raise errors, instead they will be deferred until later
  private[scala] var deferredError: Option[RuntimeException] = None


  import DurationConversions._

  def bucket(name: String): Future[AsyncBucket] = {
    FutureConversions.javaMonoToScalaFuture(core.openBucket(name))
      .map(v => new AsyncBucket(name, core, environment))
  }

  // TODO BLOCKED query
  def query(statement: String, options: QueryOptions): Future[QueryResult] = {
    val validations: Try[QueryRequest] = for {
      _ <- Validate.notNullOrEmpty(statement, "statement")
      _ <- Validate.notNull(options, "options")
      _ <- Validate.optNotNull(options.namedParameters, "namedParameters")
      _ <- Validate.optNotNull(options.positionalParameters, "positionalParameters")
      _ <- Validate.optNotNull(options.contextId, "contextId")
      _ <- Validate.optNotNull(options.credentials, "credentials")
      _ <- Validate.optNotNull(options.maxParallelism, "maxParallelism")
      _ <- Validate.optNotNull(options.disableMetrics, "disableMetrics")
      _ <- Validate.optNotNull(options.pipelineBatch, "pipelineBatch")
      _ <- Validate.optNotNull(options.pipelineCap, "pipelineCap")
      _ <- Validate.optNotNull(options.profile, "profile")
      _ <- Validate.optNotNull(options.readonly, "readonly")
      _ <- Validate.optNotNull(options.retryStrategy, "retryStrategy")
      _ <- Validate.optNotNull(options.scanCap, "scanCap")
      _ <- Validate.optNotNull(options.scanConsistency, "scanConsistency")
      _ <- Validate.optNotNull(options.serverSideTimeout, "serverSideTimeout")
      _ <- Validate.optNotNull(options.timeout, "timeout")
    } yield null

    validations match {
      case Failure(err) => Future.failed(err)
      case Success(_) =>
        val result = new QueryConsumer()

        // TODO BLOCKED proper jackson encoding with options
        val query = ("{\"statement\":\"" + statement + "\"}").getBytes(CharsetUtil.UTF_8)

        val timeout: java.time.Duration = options.timeout match {
          case Some(v) => v
          case _ => environment.timeoutConfig.queryTimeout()
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

  def shutdown(): Future[Unit] = {
    Future {
      environment.shutdown(environment.timeoutConfig().disconnectTimeout())
    }
  }
}

object AsyncCluster {
  private implicit val ec = Cluster.ec

  def connect(connectionString: String, username: String, password: String): Future[AsyncCluster] = {
    Cluster.connect(connectionString, username, password) match {
      case Success(cluster) => Future { cluster.async }
      case Failure(err) => Future.failed(err)
    }
  }

  def connect(connectionString: String, credentials: Credentials): Future[AsyncCluster] = {
    Cluster.connect(connectionString, credentials) match {
      case Success(cluster) => Future { cluster.async }
      case Failure(err) => Future.failed(err)
    }
  }

  def connect(environment: ClusterEnvironment): Future[AsyncCluster] = {
    Cluster.connect(environment) match {
      case Success(cluster) => Future { cluster.async }
      case Failure(err) => Future.failed(err)
    }
  }
}