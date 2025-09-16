/*
 * Copyright 2025 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
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
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

@NullMarked
@Stability.Internal
public abstract class AuthenticatorWrapper implements Authenticator {

  protected abstract Authenticator wrapped();

  @Override
  public void authKeyValueConnection(final EndpointContext endpointContext, final ChannelPipeline pipeline) {
    wrapped().authKeyValueConnection(endpointContext, pipeline);
  }

  @Override
  public void authHttpRequest(final ServiceType serviceType, final HttpRequest request) {
    wrapped().authHttpRequest(serviceType, request);
  }

  @Override
  public @Nullable CallCredentials protostellarCallCredentials() {
    return wrapped().protostellarCallCredentials();
  }

  @Override
  public void applyTlsProperties(final SslContextBuilder sslContextBuilder) {
    wrapped().applyTlsProperties(sslContextBuilder);
  }

  @Override
  public boolean supportsTls() {
    return wrapped().supportsTls();
  }

  @Override
  public boolean supportsNonTls() {
    return wrapped().supportsNonTls();
  }
}
