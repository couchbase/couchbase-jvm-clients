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

package com.couchbase.client.core.io.netty;

import com.couchbase.client.core.env.SecurityConfig;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufAllocator;
import com.couchbase.client.core.deps.io.netty.handler.ssl.OpenSsl;
import com.couchbase.client.core.deps.io.netty.handler.ssl.SslContextBuilder;
import com.couchbase.client.core.deps.io.netty.handler.ssl.SslHandler;
import com.couchbase.client.core.deps.io.netty.handler.ssl.SslProvider;

/**
 * This factory creates {@link SslHandler} based on a given configuration.
 *
 * @since 2.0.0
 */
public class SslHandlerFactory {

  /**
   * Checks once if OpenSSL is available.
   */
  private static final boolean OPENSSL_AVAILABLE = OpenSsl.isAvailable();

  public static SslHandler get(final ByteBufAllocator allocator, final SecurityConfig config)
    throws Exception {
    SslProvider provider =  OPENSSL_AVAILABLE ? SslProvider.OPENSSL : SslProvider.JDK;

    SslContextBuilder context = SslContextBuilder.forClient()
      .sslProvider(provider);

    if (config.trustManagerFactory() != null) {
      context.trustManager(config.trustManagerFactory());
    } else if (config.trustCertificates() != null && config.trustCertificates().length > 0) {
      context.trustManager(config.trustCertificates());
    }

    return context.build().newHandler(allocator);
  }

}
