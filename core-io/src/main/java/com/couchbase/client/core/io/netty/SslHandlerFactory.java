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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.events.io.CustomTlsCiphersEnabledEvent;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufAllocator;
import com.couchbase.client.core.deps.io.netty.handler.ssl.OpenSsl;
import com.couchbase.client.core.deps.io.netty.handler.ssl.SslContextBuilder;
import com.couchbase.client.core.deps.io.netty.handler.ssl.SslHandler;
import com.couchbase.client.core.deps.io.netty.handler.ssl.SslProvider;
import com.couchbase.client.core.endpoint.EndpointContext;
import com.couchbase.client.core.env.SecurityConfig;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.InvalidArgumentException;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import java.security.cert.X509Certificate;
import java.util.List;

/**
 * This factory creates {@link SslHandler} based on a given configuration.
 *
 * @since 2.0.0
 */
@Stability.Internal
public class SslHandlerFactory {

  private static class InitOnDemandHolder {
    /**
     * IMPORTANT: Don't access this field unless the user wants native TLS.
     * Otherwise, Netty will load the native library anyway during
     * {@link OpenSsl}'s static init. This causes a segfault on Alpine Linux
     * where glibc is not available.
     */
    private static final boolean OPENSSL_AVAILABLE = OpenSsl.isAvailable();
  }

  public static SslHandler get(final ByteBufAllocator allocator, final SecurityConfig config,
                               final EndpointContext endpointContext) throws Exception {
    SslContextBuilder context = sslContextBuilder(config.nativeTlsEnabled());

    if (config.trustManagerFactory() != null) {
      context.trustManager(config.trustManagerFactory());
    } else if (config.trustCertificates() != null && !config.trustCertificates().isEmpty()) {
      context.trustManager(config.trustCertificates().toArray(new X509Certificate[0]));
    }

    List<String> ciphers = config.ciphers();
    if (ciphers != null && !ciphers.isEmpty()) {
      context.ciphers(ciphers);
      endpointContext.environment().eventBus().publish(
        new CustomTlsCiphersEnabledEvent(ciphers, endpointContext)
      );
    }

    endpointContext.authenticator().applyTlsProperties(context);

    final SslHandler sslHandler = context.build().newHandler(
      allocator,
      endpointContext.remoteSocket().host(),
      endpointContext.remoteSocket().port()
    );

    SSLEngine sslEngine = sslHandler.engine();
    SSLParameters sslParameters = sslEngine.getSSLParameters();

    if (config.hostnameVerificationEnabled()) {
      sslParameters.setEndpointIdentificationAlgorithm("HTTPS");
    }

    sslEngine.setSSLParameters(sslParameters);

    return sslHandler;
  }

  private static SslContextBuilder sslContextBuilder(final boolean nativeTlsEnabled) {
    // Must check nativeTlsEnabled *before* OPENSSL_AVAILABLE, to prevent loading the library when not wanted
    SslProvider provider = nativeTlsEnabled && InitOnDemandHolder.OPENSSL_AVAILABLE ? SslProvider.OPENSSL : SslProvider.JDK;
    return SslContextBuilder.forClient().sslProvider(provider);
  }

  /**
   * True if the native ssl transport is available, false otherwise.
   */
  @Stability.Internal
  public static boolean opensslAvailable() {
    return InitOnDemandHolder.OPENSSL_AVAILABLE;
  }

  /**
   * Lists the default ciphers used for this platform.
   * <p>
   * Note that the list of ciphers can differ whether native TLS is enabled or not, so the parameter should reflect
   * the actual security configuration used. Native TLS is enabled by default on the configuration, so if it is not
   * overridden it should be set to true here as well.
   *
   * @param nativeTlsEnabled if native TLS is enabled on the security configuration (defaults to yes there).
   * @return the list of default ciphers.
   */
  @Stability.Internal
  public static List<String> defaultCiphers(final boolean nativeTlsEnabled) {
    if (nativeTlsEnabled && !InitOnDemandHolder.OPENSSL_AVAILABLE) {
      throw InvalidArgumentException.fromMessage("nativeTlsEnabled, but it is not available on this platform!");
    }

    try {
      SslContextBuilder builder = SslHandlerFactory.sslContextBuilder(nativeTlsEnabled);
      return builder.build().cipherSuites();
    } catch (Exception ex) {
      throw new CouchbaseException("Could not get list of default ciphers", ex);
    }
  }

}
