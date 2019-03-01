package com.couchbase.client.scala


import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

import com.couchbase.client.core.Core
import com.couchbase.client.core.env.Credentials
import com.couchbase.client.core.msg.kv.ObserveViaCasRequest
import com.couchbase.client.core.msg.query.QueryRequest
import com.couchbase.client.scala.api.QueryOptions
import com.couchbase.client.scala.env.ClusterEnvironment
import com.couchbase.client.scala.json.{JacksonTransformers, JsonObject}
import com.couchbase.client.scala.query._
import com.couchbase.client.scala.util.AsyncUtils.DefaultTimeout
import com.couchbase.client.scala.util.{FutureConversions, Validate}
import io.netty.util.CharsetUtil
import reactor.core.scala.publisher.{Flux, Mono}

import scala.compat.java8.FutureConverters
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
import collection.JavaConverters._

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

    // TODO prepared query

    validations match {
      case Failure(err) => Future.failed(err)
      case Success(_) =>
        //        val result = new QueryConsumer()

        val query = JsonObject.create
          .put("statement", statement)

        // TODO params

        Try(JacksonTransformers.MAPPER.writeValueAsString(query)) match {
          case Success(queryStr) =>
            val queryBytes = queryStr.getBytes(CharsetUtil.UTF_8)

            val timeout: Duration = options.timeout.getOrElse(environment.timeoutConfig.queryTimeout())
            val retryStrategy = options.retryStrategy.getOrElse(environment.retryStrategy())

            val request: QueryRequest = new QueryRequest(timeout,
              core.context(),
              retryStrategy,
              environment.credentials(),
              queryBytes)

            core.send(request)

            import reactor.core.publisher.{Mono => JavaMono}
            import reactor.core.scala.publisher.{Mono => ScalaMono}

            val javaMono = JavaMono.fromFuture(request.response())

            val out: JavaMono[QueryResult] = javaMono
              .flatMap(response => {

                val rowsKeeper = new AtomicReference[java.util.List[Array[Byte]]]()
                val errorsKeeper = new AtomicReference[java.util.List[Array[Byte]]]()

                val ret: JavaMono[QueryResult] = response
                  .rows().collectList().doOnNext(r => rowsKeeper.set(r))
                  .then(response.errors().collectList().doOnNext(e => errorsKeeper.set(e)))
                  .flatMap(_ => {
                    val rows = rowsKeeper.get().asScala.map(QueryRow)

                    val result = QueryResult(rows)

                    val out: JavaMono[QueryResult] = Option(errorsKeeper.get()).map(_.asScala.map(QueryError)) match {
                      case Some(err) =>
                        if (err.nonEmpty) {
                          JavaMono.error(QueryServiceException(err))
                        }
                        else {
                          JavaMono.just(result)
                        }
                      case _ =>
                        JavaMono.just(result)
                    }

                    out
                  })

                ret
              })

            FutureConversions.javaMonoToScalaFuture(out)

          case Failure(err)
          => Future.failed(err)
        }
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
    Future {
      Cluster.connect(connectionString, username, password).async
    }
  }

  def connect(connectionString: String, credentials: Credentials): Future[AsyncCluster] = {
    Future {
      Cluster.connect(connectionString, credentials).async
    }
  }

  def connect(environment: ClusterEnvironment): Future[AsyncCluster] = {
    Future {
      Cluster.connect(environment).async
    }
  }
}