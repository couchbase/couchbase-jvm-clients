package com.couchbase.client.scala


import java.util.concurrent.{CompletableFuture, TimeUnit}
import java.util.concurrent.atomic.AtomicReference

import com.couchbase.client.core.Core
import com.couchbase.client.core.env.Credentials
import com.couchbase.client.core.msg.kv.ObserveViaCasRequest
import com.couchbase.client.core.msg.query.{QueryAdditionalBasic, QueryRequest, QueryResponse}
import com.couchbase.client.scala.api.QueryOptions
import com.couchbase.client.scala.env.ClusterEnvironment
import com.couchbase.client.scala.json.JsonObject
import com.couchbase.client.scala.query._
import com.couchbase.client.scala.util.AsyncUtils.DefaultTimeout
import com.couchbase.client.scala.util.{FutureConversions, Validate}
import com.couchbase.client.core.deps.io.netty.util.CharsetUtil
import com.couchbase.client.core.deps.io.netty.util.CharsetUtil
import com.couchbase.client.core.error.QueryServiceException
import reactor.core.scala.publisher.{Flux, Mono}

import scala.compat.java8.FutureConverters
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
import collection.JavaConverters._
import scala.compat.java8.OptionConverters._

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
  private[scala] val core = Core.create(environment)
  private[scala] val env = environment

  // Opening resources will not raise errors, instead they will be deferred until later
  private[scala] var deferredError: Option[RuntimeException] = None

  private[scala] val queryHandler = new QueryHandler()


  def bucket(name: String): Future[AsyncBucket] = {
    FutureConversions.javaMonoToScalaFuture(core.openBucket(name))
      .map(v => new AsyncBucket(name, core, environment))
  }

  def query(statement: String, options: QueryOptions): Future[QueryResult] = {

    queryHandler.request(statement, options, core, environment) match {
      case Success(request) =>
        core.send(request)

        import reactor.core.scala.publisher.{Mono => ScalaMono}
        val rowsKeeper = new AtomicReference[Seq[QueryRow]]()

        val ret: Future[QueryResult] = FutureConversions.javaCFToScalaMono(request, request.response(),
          propagateCancellation = true)
          .flatMap(response => FutureConversions.javaFluxToScalaFlux(response.rows)
            .collectSeq()
            .flatMap(rows => {
              rowsKeeper.set(rows.map(QueryRow))

              FutureConversions.javaMonoToScalaMono(response.additional())
            })
            .map(addl => QueryResult(
              rowsKeeper.get(),
              response.requestId(),
              response.clientContextId().asScala,
              QuerySignature(response.signature().asScala),
              QueryAdditional(QueryMetrics.fromBytes(addl.metrics),
                addl.warnings.asScala.map(QueryError),
                addl.status,
                addl.profile.asScala.map(QueryProfile))
            ))
          )
          .onErrorResume(err => {
            err match {
              case e: QueryServiceException => ScalaMono.error(QueryError(e.content))
              case _ => ScalaMono.error(err)
            }
          }).toFuture

        ret.failed.foreach(err => {
          println(s"scala future error ${err}")
        })

        ret


      case Failure(err) => Future.failed(err)
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