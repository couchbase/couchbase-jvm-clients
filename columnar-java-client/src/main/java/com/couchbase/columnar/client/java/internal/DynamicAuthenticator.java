/*
 * Copyright 2024 Couchbase, Inc.
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

package com.couchbase.columnar.client.java.internal;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.io.grpc.CallCredentials;
import com.couchbase.client.core.deps.io.netty.channel.ChannelPipeline;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpRequest;
import com.couchbase.client.core.deps.io.netty.handler.ssl.SslContextBuilder;
import com.couchbase.client.core.endpoint.EndpointContext;
import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.service.ServiceType;

import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * Delegates all methods to the authenticator returned by the
 * given supplier.
 */
@Stability.Internal
public class DynamicAuthenticator implements Authenticator {
  private final Supplier<Authenticator> supplier;

  public DynamicAuthenticator(Supplier<Authenticator> supplier) {
    this.supplier = requireNonNull(supplier);
  }

  @Override
  public void authKeyValueConnection(final EndpointContext endpointContext, final ChannelPipeline pipeline) {
    supplier.get().authKeyValueConnection(endpointContext, pipeline);
  }

  @Override
  public void authHttpRequest(final ServiceType serviceType, final HttpRequest request) {
    supplier.get().authHttpRequest(serviceType, request);
  }

  @Override
  public CallCredentials protostellarCallCredentials() {
    return supplier.get().protostellarCallCredentials();
  }

  @Override
  public void applyTlsProperties(final SslContextBuilder sslContextBuilder) {
    supplier.get().applyTlsProperties(sslContextBuilder);
  }

  @Override
  public boolean supportsTls() {
    return supplier.get().supportsTls();
  }

  @Override
  public boolean supportsNonTls() {
    return supplier.get().supportsNonTls();
  }
}
