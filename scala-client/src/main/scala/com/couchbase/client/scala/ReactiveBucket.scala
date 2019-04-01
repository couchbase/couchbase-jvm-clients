/*
 * Copyright (c) 2019 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.scala

import com.couchbase.client.core.error.ViewServiceException
import com.couchbase.client.core.msg.view.{ViewRequest, ViewResponse}
import com.couchbase.client.scala.query.handlers.{SpatialViewHandler, ViewHandler}
import com.couchbase.client.scala.query.{QueryOptions, ReactiveQueryResult}
import com.couchbase.client.scala.util.{AsyncUtils, FutureConversions}
import com.couchbase.client.scala.view._
import reactor.core.scala.publisher.Mono
import reactor.core.scala.publisher.{Flux => ScalaFlux, Mono => ScalaMono}

import scala.compat.java8.OptionConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/** Represents a Couchbase bucket resource.
  *
  * This is the reactive version of the [[Bucket]] API.
  *
  * Applications should not create these manually, but instead use the functions in [[Cluster]].
  *
  * @param name the name of this bucket
  * @param ec   an ExecutionContext to use for any Future.  Will be supplied automatically as long as resources are
  *             opened in the normal way, starting from functions in [[Cluster]]
  *
  * @author Graham Pople
  * @since 1.0.0
  */
class ReactiveBucket private[scala](val async: AsyncBucket)
                                   (implicit ec: ExecutionContext) {
  private[scala] val viewHandler = new ViewHandler
  private[scala] val spatialViewHandler = new SpatialViewHandler

  /** Opens and returns a Couchbase scope resource.
    *
    * @param name the name of the scope
    */
  def scope(name: String): Mono[ReactiveScope] = {
    Mono.fromFuture(async.scope(name)).map(v => new ReactiveScope(v, async.name))
  }

  /** Opens and returns the default Couchbase scope. */
  def defaultScope: Mono[ReactiveScope] = {
    scope(DefaultResources.DefaultScope)
  }

  /** Returns the Couchbase default collection resource. */
  def defaultCollection: Mono[ReactiveCollection] = {
    scope(DefaultResources.DefaultScope).flatMap(v => v.defaultCollection)
  }

  /** Opens a Couchbase collection resource on the default scope.
    *
    * @param collection the name of the collection
    *
    * @return a created collection resource
    */
  def collection(name: String): Mono[ReactiveCollection] = {
    scope(DefaultResources.DefaultScope).flatMap(v => v.collection(name))
  }

  /** Performs a view query against the cluster.
    *
    * This is a reactive streaming version of this API.  See [[Bucket]] for a synchronous blocking version, and
    * [[Bucket.async]] for an async version.
    *
    * @param designDoc the view design document to use
    * @param viewName  the view to use
    * @param options   any view query options - see [[ViewOptions]] for documentation
    *
    * @return a `Mono` containing a [[ViewResult]] (which includes any returned rows)
    */
  def viewQuery(designDoc: String,
                viewName: String,
                options: ViewOptions = ViewOptions()): Mono[ReactiveViewResult] = {
    val req = viewHandler.request(designDoc, viewName, options, async.core, async.environment, async.name)
    viewQuery(req)
  }

  /** Performs a spatial view query against the cluster.
    *
    * This is a reactive streaming version of this API.  See [[Bucket]] for a synchronous blocking version, and
    * [[Bucket.async]] for an async version.
    *
    * @param designDoc the view design document to use
    * @param viewName  the view to use
    * @param options   any spatial view query options - see [[SpatialViewOptions]] for documentation
    *
    * @return a `Mono` containing a [[ViewResult]] (which includes any returned rows)
    */
  def spatialViewQuery(designDoc: String,
                       viewName: String,
                       options: SpatialViewOptions = SpatialViewOptions()) = {
    val req = spatialViewHandler.request(designDoc, viewName, options, async.core, async.environment, async.name)
    viewQuery(req)
  }

  private def viewQuery(req: Try[ViewRequest]): Mono[ReactiveViewResult] = {
    req match {
      case Failure(err) =>
        Mono.error(err)

      case Success(request) =>

        FutureConversions.javaCFToScalaMono(request, request.response(), false)
          .map(response => {

            val rows: ScalaFlux[ViewRow] = FutureConversions.javaFluxToScalaFlux(response.rows())

              .map[ViewRow](bytes => ViewRow(bytes.data()))

              .flatMap(_ => FutureConversions.javaMonoToScalaMono(response.trailer()))

              // Check for errors
              .flatMap(trailer => {
              trailer.error().asScala match {
                case Some(err) =>
                  val msg = "Encountered view error '" + err.error() + "' with reason '" + err.reason() + "'"
                  val error = new ViewServiceException(msg)
                  Mono.error(error)
                case _ => Mono.empty
              }
            })


            val meta = ViewMeta(
              response.header().debug().asScala.map(v => ViewDebug(v)),
              response.header().totalRows())

            ReactiveViewResult(meta, rows)
          })
    }
  }
}
