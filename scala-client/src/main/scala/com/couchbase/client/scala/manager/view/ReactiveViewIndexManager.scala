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
package com.couchbase.client.scala.manager.view

import java.nio.charset.StandardCharsets.UTF_8
import com.couchbase.client.core.Core
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode
import com.couchbase.client.core.deps.io.netty.handler.codec.http._
import com.couchbase.client.core.endpoint.http.{
  CoreCommonOptions,
  CoreHttpPath,
  CoreHttpRequest,
  CoreHttpResponse
}
import com.couchbase.client.core.error.{
  CouchbaseException,
  DesignDocumentNotFoundException,
  ViewServiceException
}
import com.couchbase.client.core.json.Mapper
import com.couchbase.client.core.logging.RedactableArgument.redactMeta
import com.couchbase.client.core.msg.{RequestTarget, ResponseStatus}
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.core.util.UrlQueryStringBuilder.urlEncode
import com.couchbase.client.scala.manager.ManagerUtil
import com.couchbase.client.scala.transformers.JacksonTransformers
import com.couchbase.client.scala.util.DurationConversions._
import com.couchbase.client.scala.util.FutureConversions
import com.couchbase.client.scala.view.DesignDocumentNamespace
import reactor.core.scala.publisher.{SFlux, SMono}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

class ReactiveViewIndexManager(private[scala] val core: Core, bucket: String) {
  private val DefaultTimeout: Duration =
    core.context().environment().timeoutConfig().managementTimeout()
  private val DefaultRetryStrategy: RetryStrategy = core.context().environment().retryStrategy()
  private val httpClient                          = core.httpClient(RequestTarget.views(bucket))

  def getDesignDocument(
      designDocName: String,
      namespace: DesignDocumentNamespace,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): SMono[DesignDocument] = {
    val options = CoreCommonOptions.of(timeout, retryStrategy, null)
    pathForDesignDocument(designDocName, namespace) match {
      case Success(path) =>
        sendRequest(httpClient.get(CoreHttpPath.path(path), options).build())
          .onErrorResume(err => SMono.error(mapNotFoundError(err, designDocName, namespace)))
          .flatMap(response => {
            response.status match {
              case ResponseStatus.SUCCESS =>
                val parsed: Try[DesignDocument] = Try {
                  upickle.default.read[ujson.Obj](response.content)
                }.flatMap(json => ReactiveViewIndexManager.parseDesignDocument(designDocName, json))

                parsed match {
                  case Success(designDoc) => SMono.just(designDoc)
                  case Failure(err)       => SMono.error(err)
                }
              case _ =>
                SMono.error(
                  new CouchbaseException(
                    "Failed to drop design document [" +
                      redactMeta(designDocName) + "] from namespace " + namespace
                  )
                )
            }
          })
      case Failure(err) =>
        SMono.error(err)
    }
  }

  def getAllDesignDocuments(
      namespace: DesignDocumentNamespace,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): SFlux[DesignDocument] = {
    // This particular request goes to port 8091 not 8092, hence use of ManagerUtil.getRequest
    ManagerUtil
      .sendRequest(core, HttpMethod.GET, pathForAllDesignDocuments, timeout, retryStrategy)
      .flatMapMany(response => {
        response.status match {
          case ResponseStatus.SUCCESS =>
            ReactiveViewIndexManager
              .parseAllDesignDocuments(new String(response.content(), UTF_8), namespace) match {
              case Success(docs) => SFlux.fromIterable(docs)
              case Failure(err)  => SFlux.error(err)
            }
          case _ =>
            SFlux.error(
              new CouchbaseException(
                "Failed to get all design documents; response status=" + response.status + "; response body=" + new String(
                  response.content,
                  UTF_8
                )
              )
            )

        }
      })
  }

  def upsertDesignDocument(
      indexData: DesignDocument,
      namespace: DesignDocumentNamespace,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): SMono[Unit] = {
    val options = CoreCommonOptions.of(timeout, retryStrategy, null)
    pathForDesignDocument(indexData.name, namespace) match {
      case Success(path) =>
        val body = toJson(indexData)
        val request = httpClient
          .put(CoreHttpPath.path(path), options)
          .json(Mapper.encodeAsBytes(body))
          .build()

        SMono.defer(() => {
          core.send(request)
          FutureConversions
            .javaCFToScalaMono(request, request.response(), propagateCancellation = true)
            .doOnNext(_ => request.context.logicallyComplete)
            .doOnError(err => request.context().logicallyComplete(err))
            .map(_ => ())
        })
      case Failure(err) =>
        SMono.error(err)
    }
  }

  def dropDesignDocument(
      designDocName: String,
      namespace: DesignDocumentNamespace,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): SMono[Unit] = {
    val options = CoreCommonOptions.of(timeout, retryStrategy, null)
    pathForDesignDocument(designDocName, namespace) match {
      case Success(path) =>
        sendRequest(httpClient.delete(CoreHttpPath.path(path), options).build())
          .onErrorResume(err => SMono.error(mapNotFoundError(err, designDocName, namespace)))
          .flatMap(response => {
            response.status match {
              case ResponseStatus.SUCCESS => SMono.just(())
              case _ =>
                SMono.error(
                  new CouchbaseException(
                    "Failed to drop design document [" +
                      redactMeta(designDocName) + "] from namespace " + namespace
                  )
                )
            }
          })
      case Failure(err) =>
        SMono.error(err)
    }
  }

  def mapNotFoundError(
      in: Throwable,
      designDocName: String,
      namespace: DesignDocumentNamespace
  ): Throwable = {
    def default = () => {
      new CouchbaseException(
        s"Failed to drop design document [${redactMeta(designDocName)}] from namespace $namespace"
      )
    }

    in match {
      case x: ViewServiceException =>
        if (x.content.contains("not_found")) {
          DesignDocumentNotFoundException.forName(designDocName, namespace.toString)
        } else default()
      case _ => default()
    }
  }

  def publishDesignDocument(
      designDocName: String,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): SMono[Unit] = {
    getDesignDocument(designDocName, DesignDocumentNamespace.Development, timeout, retryStrategy)
      .map(
        doc => upsertDesignDocument(doc, DesignDocumentNamespace.Production, timeout, retryStrategy)
      )
  }

  private def pathForDesignDocument(
      name: String,
      namespace: DesignDocumentNamespace
  ): Try[String] = {
    DesignDocumentNamespace
      .requireUnqualified(name)
      .map(unqualifiedName => {
        val adjusted = namespace.adjustName(unqualifiedName)
        "/" + urlEncode(bucket) + "/_design/" + urlEncode(adjusted)
      })
  }

  private def pathForAllDesignDocuments = {
    "/pools/default/buckets/" + urlEncode(bucket) + "/ddocs"
  }

  private def toJson(doc: DesignDocument): ObjectNode = {
    val root  = JacksonTransformers.MAPPER.createObjectNode
    val views = root.putObject("views")
    doc.views.foreach(x => {
      val key      = x._1
      val value    = x._2
      val viewNode = JacksonTransformers.MAPPER.createObjectNode
      viewNode.put("map", value.map)
      value.reduce.foreach((r: String) => viewNode.put("reduce", r))
      views.set(key, viewNode)
      ()
    })
    root
  }

  private def sendRequest(request: CoreHttpRequest): SMono[CoreHttpResponse] = {
    SMono.defer(() => {
      core.send(request)
      FutureConversions
        .wrap(request, request.response, propagateCancellation = true)
        .doOnNext(_ => request.context.logicallyComplete)
        .doOnError(err => request.context().logicallyComplete(err))
    })
  }
}

object ReactiveViewIndexManager {
  private[scala] def parseAllDesignDocuments(
      in: String,
      namespace: DesignDocumentNamespace
  ): Try[ArrayBuffer[DesignDocument]] = {
    Try {
      val json = upickle.default.read[ujson.Obj](in)
      val rows = json("rows").arr
      rows
        .map(row => {
          val doc           = row("doc").obj
          val metaId        = doc("meta").obj("id").str
          val designDocName = metaId.stripPrefix("_design/")
          if (namespace.contains(designDocName)) {
            val designDoc = doc("json").obj
            parseDesignDocument(designDocName, designDoc).toOption
          } else None
        })
        .filter(_.isDefined)
        .map(_.get)
    }
  }

  private[scala] def parseDesignDocument(name: String, node: ujson.Obj): Try[DesignDocument] = {
    Try {
      val views = node("views").obj
      val v: collection.Map[String, View] = views.map(n => {
        val viewName   = n._1
        val viewMap    = n._2.obj("map").str
        val viewReduce = n._2.obj.get("reduce").map(_.str)
        viewName -> View(viewMap, viewReduce)
      })
      DesignDocument(name.stripPrefix("dev_"), v)
    }
  }
}
