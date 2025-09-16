/*
 * Copyright (c) 2018 Couchbase, Inc.
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

package com.couchbase.client.core.env;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.io.grpc.CallCredentials;
import com.couchbase.client.core.deps.io.netty.channel.ChannelPipeline;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpRequest;
import com.couchbase.client.core.deps.io.netty.handler.ssl.SslContextBuilder;
import com.couchbase.client.core.endpoint.EndpointContext;
import com.couchbase.client.core.service.ServiceType;
import reactor.util.annotation.Nullable;

/**
 * An authentication strategy.
 * <p>
 * <b>Note:</b> The methods of this interface are part of the SDK's internal API, and may change at any time.
 * Implementing this interface yourself is not recommended. Please use one of the provided implementations.
 *
 * @see PasswordAuthenticator
 * @see CertificateAuthenticator
 * @see DelegatingAuthenticator
 *
 * @since 2.0.0
 */
public interface Authenticator {

  /**
   * Allows the authenticator to add KV handlers during connection bootstrap to perform
   * authentication.
   *
   * @param endpointContext the endpoint context.
   * @param pipeline the pipeline when the endpoint is constructed.
   */
  @Stability.Internal
  default void authKeyValueConnection(final EndpointContext endpointContext, final ChannelPipeline pipeline) { }

  /**
   * Allows to add authentication credentials to the http request for the given service.
   *
   * @param serviceType the service for this request.
   * @param request the http request.
   */
  @Stability.Internal
  default void authHttpRequest(final ServiceType serviceType, final HttpRequest request) { }

  @Nullable
  @Stability.Internal
  CallCredentials protostellarCallCredentials();

  /**
   * The authenticator gets the chance to attach the client certificate to the ssl context if needed.
   *
   * @param sslContextBuilder the netty context builder
   */
  @Stability.Internal
  default void applyTlsProperties(final SslContextBuilder sslContextBuilder) { }

  @Stability.Internal
  default boolean requiresTls() {
    return true;
  }

}
