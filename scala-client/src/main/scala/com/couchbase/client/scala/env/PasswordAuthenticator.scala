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
package com.couchbase.client.scala.env

import com.couchbase.client.core.deps.io.grpc.Metadata
import com.couchbase.client.core.deps.io.netty.channel.ChannelPipeline
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpRequest
import com.couchbase.client.core.endpoint.EndpointContext
import com.couchbase.client.core.env.Authenticator
import com.couchbase.client.core.service.ServiceType

case class PasswordAuthenticator(username: String, password: String) extends Authenticator {

  private val inner = com.couchbase.client.core.env.PasswordAuthenticator.create(username, password)

  /**
    * Allows the authenticator to add KV handlers during connection bootstrap to perform
    * authentication.
    *
    * @param endpointContext the endpoint context.
    * @param pipeline        the pipeline when the endpoint is constructed.
    */
  override def authKeyValueConnection(
      endpointContext: EndpointContext,
      pipeline: ChannelPipeline
  ): Unit = {
    inner.authKeyValueConnection(endpointContext, pipeline)
  }

  /**
    * Allows to add authentication credentials to the http request for the given service.
    *
    * @param serviceType the service for this request.
    * @param request     the http request.
    */
  override def authHttpRequest(serviceType: ServiceType, request: HttpRequest): Unit = {
    inner.authHttpRequest(serviceType, request)
  }

    override def authProtostellarRequest(metadata: Metadata): Unit = inner.authProtostellarRequest(metadata)
}
