package com.couchbase.client.scala


import java.util.concurrent.{CompletableFuture, TimeUnit}
import java.util.concurrent.atomic.AtomicReference

import com.couchbase.client.core.Core
import com.couchbase.client.core.env.Credentials
import com.couchbase.client.core.msg.kv.ObserveViaCasRequest
import com.couchbase.client.core.msg.query.QueryRequest
import com.couchbase.client.scala.api.QueryOptions
import com.couchbase.client.scala.env.ClusterEnvironment
import com.couchbase.client.scala.json.JsonObject
import com.couchbase.client.scala.query._
import com.couchbase.client.scala.util.AsyncUtils.DefaultTimeout
import com.couchbase.client.scala.util.{FutureConversions, Validate}
import com.couchbase.client.core.deps.io.netty.util.CharsetUtil
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

        import reactor.core.publisher.{Mono => JavaMono}
        import reactor.core.scala.publisher.{Mono => ScalaMono}

        val javaMono = JavaMono.fromFuture(request.response())

        // Wait until rows and others are done
        // If rows failed with an error, return Future.failed
        // Else return Future(QueryResult)

        val out: JavaMono[QueryResult] = javaMono
          .flatMap(response => {

            //            val requestIdKeeper = new AtomicReference[String]()
            //            val clientContextIdKeeper = new AtomicReference[String]()
            //            val rowsKeeper = new AtomicReference[java.util.List[Array[Byte]]]()
            val errorsKeeper = new AtomicReference[Throwable]()
            val warningsKeeper = new AtomicReference[java.util.List[Array[Byte]]]()
            val metricsKeeper = new AtomicReference[QueryMetrics]()

            val ret: JavaMono[QueryResult] = response.rows
              .doOnError(err => {
                println("got error " + err)
                errorsKeeper.set(err)
              })
              .collectList()
              //              .doOnNext(r => rowsKeeper.set(r))
              //              .requestId().doOnNext(v => requestIdKeeper.set(v))
              //              .`then`(response.clientContextId().doOnNext(v => clientContextIdKeeper.set(v)))
              //              .(response.rows().collectList().doOnNext(r => rowsKeeper.set(r))

              //              .`then`(response.errors().collectList().doOnNext(e => errorsKeeper.set(e)))
              //              .`then`(response.warnings().collectList().doOnNext(v => warningsKeeper.set(v)))
              //              .`then`(response.metrics().doOnNext(v => metricsKeeper.set(QueryMetrics.fromBytes(v))))
              .map(rowsRaw => {
              val rows = rowsRaw.asScala.map(QueryRow)

              val result = QueryResult(
                rows,
                response.requestId(),
                response.clientContextId().asScala,
                QueryOther(
                  null, null
                  //                    metricsKeeper.get(),
                  //                    warningsKeeper.get().asScala.map(w => QueryError(w))
                ))

              //                val out: JavaMono[QueryResult] = Option(errorsKeeper.get()) match {
              //                  case Some(err) =>
              //                      JavaMono.error(err)
              //                  case _ =>
              //                    JavaMono.just(result)
              //                }

              result
            })

            ret
          })

        val future: CompletableFuture[QueryResult] = out.toFuture

        val ret = FutureConversions.javaCFToScalaFuture(future)

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