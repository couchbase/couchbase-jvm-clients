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

package com.couchbase.client.scala.manager.collection

import java.nio.charset.StandardCharsets

import com.couchbase.client.core.config.CollectionsManifest
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpMethod
import com.couchbase.client.core.error._
import com.couchbase.client.core.msg.ResponseStatus
import com.couchbase.client.core.msg.manager.GenericManagerResponse
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.core.util.UrlQueryStringBuilder
import com.couchbase.client.core.util.UrlQueryStringBuilder.urlEncode
import com.couchbase.client.scala.AsyncBucket
import com.couchbase.client.scala.manager.ManagerUtil
import com.couchbase.client.scala.transformers.JacksonTransformers
import com.couchbase.client.scala.util.DurationConversions.javaDurationToScala
import reactor.core.scala.publisher.{SFlux, SMono}

import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

class ReactiveCollectionManager(private[scala] val bucket: AsyncBucket) {
  private val core = bucket.core
  private[scala] val defaultManagerTimeout = javaDurationToScala(
    core.context().environment().timeoutConfig().managementTimeout()
  )
  private[scala] val defaultRetryStrategy = core.context().environment().retryStrategy()

  private[scala] def collectionExists(
      collection: CollectionSpec,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): SMono[Boolean] = {
    loadManifest(timeout, retryStrategy)
      .map(manifest => {
        val foundCollection = manifest
          .scopes()
          .asScala
          .find(_.name == collection.scopeName)
          .flatMap(
            scope =>
              scope.collections.asScala
                .find(_.name == collection.name)
          )

        foundCollection.isDefined
      })
  }

  private[scala] def scopeExists(
      scopeName: String,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): SMono[Boolean] = {
    loadManifest(timeout, retryStrategy)
      .map(_.scopes().asScala.exists(_.name == scopeName))
  }

  def getScope(
      scopeName: String,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): SMono[ScopeSpec] = {
    getAllScopes(timeout, retryStrategy)
      .collectSeq()
      .flatMap(scopes => {
        scopes.find(_.name == scopeName) match {
          case Some(scope) => SMono.just(scope)
          case _           => SMono.raiseError(new ScopeNotFoundException(scopeName))
        }
      })
  }

  def getAllScopes(
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): SFlux[ScopeSpec] = {
    loadManifest(timeout, retryStrategy)
      .flatMapMany(manifest => {
        val scopes = manifest
          .scopes()
          .asScala
          .map(scope => {
            val collections: collection.Seq[CollectionSpec] = scope.collections.asScala
              .map(coll => CollectionSpec(coll.name, scope.name))

            ScopeSpec(scope.name, collections)
          })
        SFlux.fromIterable(scopes)
      })
  }

  def createCollection(
      collection: CollectionSpec,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): SMono[Unit] = {
    val body = UrlQueryStringBuilder.create.add("name", collection.name)
    val path = pathForScope(bucket.name, collection.scopeName)

    ManagerUtil
      .sendRequest(core, HttpMethod.POST, path, body, timeout, retryStrategy)
      .flatMap(response => {
        SMono.fromTry(checkForErrors(response, collection.scopeName, collection.name))
      })
  }

  def dropCollection(
      collection: CollectionSpec,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): SMono[Unit] = {
    val path = pathForCollection(bucket.name, collection.scopeName, collection.name)

    ManagerUtil
      .sendRequest(core, HttpMethod.DELETE, path, timeout, retryStrategy)
      .flatMap(response => {
        SMono.fromTry(checkForErrors(response, collection.scopeName, collection.name))
      })
  }

  def createScope(
      scopeName: String,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): SMono[Unit] = {
    val body = UrlQueryStringBuilder.create.add("name", scopeName)
    val path = pathForManifest(bucket.name)

    ManagerUtil
      .sendRequest(core, HttpMethod.POST, path, body, timeout, retryStrategy)
      .flatMap(response => {
        SMono.fromTry(checkForErrors(response, scopeName, collectionName = null))
      })
  }

  def dropScope(
      scopeName: String,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): SMono[Unit] = {
    val path = pathForScope(bucket.name, scopeName)

    ManagerUtil
      .sendRequest(core, HttpMethod.DELETE, path, timeout, retryStrategy)
      .flatMap(response => {
        SMono.fromTry(checkForErrors(response, scopeName, collectionName = null))
      })
  }

  private def pathForScope(bucketName: String, scopeName: String) = {
    pathForManifest(bucketName) + "/" + urlEncode(scopeName)
  }

  private def pathForCollection(bucketName: String, scopeName: String, collectionName: String) = {
    pathForScope(bucketName, scopeName) + "/" + urlEncode(collectionName)
  }

  private def pathForManifest(bucketName: String) = {
    "/pools/default/buckets/" + urlEncode(bucketName) + "/collections"
  }

  private def loadManifest(
      timeout: Duration,
      retryStrategy: RetryStrategy
  ): SMono[CollectionsManifest] = {
    ManagerUtil
      .sendRequest(core, HttpMethod.GET, pathForManifest(bucket.name), timeout, retryStrategy)
      .flatMap(response => {
        val error = new String(response.content, StandardCharsets.UTF_8)

        if (response.status == ResponseStatus.INVALID_ARGS) {
          if (error.contains("Not allowed on this version of cluster")) {
            SMono.raiseError(
              new IllegalArgumentException(
                "This version of Couchbase Server does not support this operation"
              )
            )
          } else {
            SMono.raiseError(new IllegalArgumentException(error))
          }
        } else {
          SMono.fromTry(
            Try(
              JacksonTransformers.MAPPER.readValue(response.content(), classOf[CollectionsManifest])
            )
          )
        }
      })
  }

  /**
    * Helper method to check for common errors and raise the right exceptions in those cases.
    *
    * @param response the response to check.
    */
  private def checkForErrors(
      response: GenericManagerResponse,
      scopeName: String,
      collectionName: String
  ): Try[Unit] = {
    if (response.status.success) {
      Success(())
    } else {
      val error = new String(response.content, StandardCharsets.UTF_8)

      if (response.status == ResponseStatus.NOT_FOUND) {
        if (error.contains("Scope with this name is not found")) {
          Failure(new ScopeNotFoundException(scopeName))
        } else if (error.contains("Collection with this name is not found")) {
          Failure(new CollectionNotFoundException(collectionName))
        } else {
          Failure(new CouchbaseException("Unknown error in CollectionManager: " + error))
        }
      } else if (response.status == ResponseStatus.INVALID_ARGS) {
        if (error.contains("Scope with this name already exists")) {
          Failure(new ScopeExistsException(scopeName))
        } else if (error.contains("Collection with this name already exists")) {
          Failure(new CollectionExistsException(collectionName))
        } else {
          Failure(new IllegalArgumentException("Unknown error in CollectionManager: " + error))
        }
      } else {
        Failure(new CouchbaseException("Unknown error in CollectionManager: " + error))
      }
    }
  }
}
