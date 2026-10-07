/*
 * Copyright 2025-2026 Couchbase, Inc.
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

package com.couchbase.client.core.service;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.env.IoConfig;
import com.couchbase.client.core.env.SecurityConfig;
import com.couchbase.client.core.msg.RequestContext;
import com.couchbase.client.core.util.CbThreads;
import kotlin.reflect.KVariance;
import okhttp3.Call;
import okhttp3.Connection;
import okhttp3.ConnectionPool;
import okhttp3.Dispatcher;
import okhttp3.EventListener;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Request;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.net.Proxy;
import java.net.Socket;
import java.net.SocketException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static com.couchbase.client.core.service.OkHttpTlsHelper.configureTls;
import static com.couchbase.client.core.service.ServiceType.KV;
import static com.couchbase.client.core.util.CbCollections.listOf;
import static com.couchbase.client.core.util.CbCollections.setOf;
import static java.util.Objects.requireNonNull;

@NullMarked
@Stability.Internal
public class CouchbaseOkHttpClient implements Closeable {
  private static final Logger log = LoggerFactory.getLogger(CouchbaseOkHttpClient.class);

  private static class DispatchState {
    volatile boolean requestStarted;

    final Duration socketReadTimeout;

    /**
     * The socket the request was sent on, once it's been sent.
     */
    volatile @Nullable Socket socket;

    DispatchState(Duration socketReadTimeout) {
      this.socketReadTimeout = requireNonNull(socketReadTimeout);
    }
  }

  static boolean requestStarted(Request request) {
    DispatchState dispatchState = request.tag(DispatchState.class);
    if (dispatchState == null) {
      log.warn("dispatch state is null; this is a bug.", new RuntimeException("missing dispatch state tag"));
      return true; // assume the worst
    }
    return dispatchState.requestStarted;
  }

  /**
   * Returns a new call for the request.
   * <p>
   * Builds the request from the given builder, which mustn't be used again.
   * <p>
   * This is the only way to make a call with this client, so every request has the state the client
   * relies on (for example, the socket read timeout).
   *
   * @param requestTimeout the SDK's timeout for the request, which the socket read timeout is based on.
   */
  public Call newCall(okhttp3.Request.Builder requestBuilder, RequestContext requestContext, Duration requestTimeout) {
    return client.newCall(newRequest(requestBuilder, requestContext, requestTimeout));
  }

  /**
   * Builds the request, with the tags {@link #newCall} adds.
   * Package-private only for tests that send requests with {@link #getClientForTest()}.
   */
  static Request newRequest(okhttp3.Request.Builder requestBuilder, RequestContext requestContext, Duration requestTimeout) {
    return requestBuilder
      .tag(DispatchState.class, new DispatchState(socketReadTimeout(requestTimeout)))
      .tag(RequestContext.class, requestContext) // so the connection tracker can record dispatch details
      .build();
  }

  private final OkHttpClient client;

  /**
   * Only for tests. Other code must use {@link #newCall}, so every request has the state the client relies on.
   */
  OkHttpClient getClientForTest() {
    return client;
  }

  /**
   * Added to the request timeout to get the socket read timeout, so the SDK's own timeout
   * fires first (and reports the right exception) instead of OkHttp's.
   */
  private static final Duration READ_TIMEOUT_GRACE_PERIOD = Duration.ofSeconds(15);

  /**
   * @param ioConfig for the idle connection timeout (how long a connection may stay idle in the pool before
   * it's closed; zero means never, like the Netty endpoints), and the TCP keepalive settings.
   */
  public CouchbaseOkHttpClient(
    Duration connectTimeout,
    IoConfig ioConfig,
    boolean nativeIoEnabled,
    SecurityConfig securityConfig,
    Authenticator credential,
    String userAgent
  ) {
    // Bump sub-millisecond timeouts up to one millisecond, otherwise OkHttp rounds down to zero (no timeout).
    connectTimeout = atLeastOneMillisecond(connectTimeout);

    Dispatcher dispatcher = new Dispatcher(CbThreads.unboundedExecutorService("cb-okhttp-dispatcher-"));

    OkHttpClient.Builder clientBuilder = new OkHttpClient.Builder()
      .connectTimeout(connectTimeout)
      .dispatcher(dispatcher)

      // Applies configured socket options
      .socketFactory(new CouchbaseSocketFactory(ioConfig, nativeIoEnabled))

      // Connect directly, like the Netty implementation. Otherwise OkHttp would use the JVM's default ProxySelector,
      // which honors proxy system properties (http.proxyHost, socksProxyHost, and so on) that an application may set
      // for its own outbound traffic, not for Couchbase's. (And through a SOCKS proxy, TCP_USER_TIMEOUT can't be set.)
      .proxy(Proxy.NO_PROXY)

      // Don't let OkHttp retry failed requests by itself; the SDK's retry orchestrator decides, like with the Netty
      // implementation. Then every retry is visible to the SDK (retry reasons, backoff, timeouts, events, and node
      // health tracking), and a non-idempotent request is never resent after it started.
      // The SDK retries connect failures (the request didn't start), and idempotent requests that failed in flight.
      .retryOnConnectionFailure(false)

      // HTTP/1.1 only, like the Netty implementation. Otherwise OkHttp offers HTTP/2 during the TLS handshake,
      // and a server that accepts it would multiplex requests over one connection. Then a streamed response
      // whose subscriber falls behind (so we stop reading it, for backpressure) could use up the connection's
      // flow-control window, stalling every other request on that connection. With HTTP/1.1, each request
      // has its own connection, so backpressure only affects that request.
      //
      // HTTP/2 would also enable connection coalescing (reusing a connection for another host the certificate
      // covers), which OkHttp decides using the trust manager it was built with (see OkHttpTlsHelper),
      // not the current one, so it could be wrong after the trusted certificates change.
      //
      // And the network interceptor below sets each request's read timeout on the connection's socket (SO_TIMEOUT).
      // With HTTP/2, every request on the connection shares the socket, so one request's timeout would apply to all.
      .protocols(listOf(Protocol.HTTP_1_1))

      // For parity with the Netty ViewService, return redirects to the caller instead of following them.
      // For example, a view request redirected with 302 is retried (VIEWS_NO_ACTIVE_PARTITION).
      // When the view service goes away, we can consider re-enabling automatic redirect handling.
      .followRedirects(false)
      .followSslRedirects(false)

      // No OkHttp read or write timeouts. OkHttp enforces them with Okio's AsyncTimeout, which costs
      // a trip through a global lock for every socket read and write, and often wakes Okio's watchdog thread.
      // Instead, the network interceptor below sets the socket's own read timeout (SO_TIMEOUT), which the
      // JDK enforces on the thread doing the read. (OkHttp sets SO_TIMEOUT too, to the same value as its
      // read timeout, so this is the same timeout without the extra bookkeeping.)
      //
      // The TLS handshake gets the connect timeout as its read timeout (see PerConnectionSslSocketFactory).
      // Request writes have no timeout of their own, like the Netty implementation. The SDK's request timeout
      // still applies: it cancels the call, which closes the socket.
      .readTimeout(Duration.ZERO)
      .writeTimeout(Duration.ZERO)

      // No limit on the number of idle connections. Each service limits the requests in flight to its node
      // (one connection each), which bounds the connections to each node, however many nodes there are.
      // Idle connections are closed after the same idle timeout as the Netty endpoints.
      .connectionPool(new ConnectionPool(
        Integer.MAX_VALUE,
        keepAlive(ioConfig.idleHttpConnectionTimeout()).toMillis(), TimeUnit.MILLISECONDS
      ))
      .addInterceptor(chain -> {
        okhttp3.Request.Builder requestBuilder = chain.request().newBuilder()
          .header("User-Agent", userAgent);

        // Don't ask for compressed responses, like the Netty implementation. Otherwise OkHttp would ask for gzip
        // (and decompress the response itself) for any request that doesn't say otherwise.
        if (chain.request().header("Accept-Encoding") == null) {
          requestBuilder.header("Accept-Encoding", "identity");
        }

        // get the value every time in case the credential is dynamic (deprecated)
        String authorizationHeaderValue = credential.getAuthHeaderValue();
        if (authorizationHeaderValue != null) {
          requestBuilder.header("Authorization", authorizationHeaderValue);
        }

        return chain.proceed(requestBuilder.build());
      })
      .addNetworkInterceptor(chain -> {
        // Runs once the request has a connection, after OkHttp has reset the socket's read timeout.
        Connection connection = requireNonNull(chain.connection(), "network interceptor has no connection");
        Socket socket = connection.socket();
        DispatchState s = chain.request().tag(DispatchState.class);
        if (s == null) {
          // Without it, the socket would have no read timeout, so a read could block forever.
          throw new IllegalStateException("Request is missing DispatchState tag; it wasn't sent with newCall(). This is a bug.");
        }
        socket.setSoTimeout(toMillisInt(s.socketReadTimeout));
        s.socket = socket;
        return chain.proceed(chain.request());
      })
      .eventListenerFactory(call -> new EventListener() {
          @Override
          public void requestHeadersStart(Call call) {
            DispatchState s = call.tag(DispatchState.class);
            if (s != null) {
              s.requestStarted = true;
            } else {
              log.warn("Request is missing DispatchState tag");
            }
          }
      });

    // Like the Netty endpoints, capture the traffic of the HTTP services the config asks for.
    if (ioConfig.servicesToCapture().stream().anyMatch(it -> it != KV)) {
      clientBuilder.addNetworkInterceptor(new TrafficCaptureInterceptor(ioConfig.servicesToCapture()));
    }

    // Asks the authenticator for the client certificate (if any) for each new connection, like the Netty implementation.
    configureTls(
      clientBuilder,
      credential::getKeyManagerFactory,
      securityConfig,
      connectTimeout // for the TLS handshake
    );

    this.client = clientBuilder.build();

    // Services limit the number of requests in flight to each node themselves, and retry
    // (possibly on a different node) when the limit is reached. Don't let the dispatcher
    // queue calls; a queued call just waits for a node that is already struggling.
    this.client.dispatcher().setMaxRequests(Integer.MAX_VALUE);
    this.client.dispatcher().setMaxRequestsPerHost(Integer.MAX_VALUE);
  }

  private static final Duration ONE_MILLISECOND = Duration.ofMillis(1);

  private static Duration atLeastOneMillisecond(Duration d) {
    return d.toMillis() > 0 ? d : ONE_MILLISECOND;
  }

  /**
   * Effectively "never". OkHttp requires a positive keep-alive, so it can't be zero.
   */
  static final Duration KEEP_ALIVE_FOREVER = Duration.ofDays(365L * 100);

  /**
   * Returns the connection pool keep-alive for the given idle connection timeout. Zero (or less) means
   * never close idle connections, as with the Netty endpoints, which don't check for idle connections then.
   */
  static Duration keepAlive(Duration idleConnectionTimeout) {
    return idleConnectionTimeout.isZero() || idleConnectionTimeout.isNegative()
      ? KEEP_ALIVE_FOREVER
      : atLeastOneMillisecond(idleConnectionTimeout);
  }

  /**
   * The server doesn't start sending a response until it's done executing the request (for example, a query).
   * Base the socket read timeout on the request timeout, so the read doesn't time out before the request does.
   * Add a grace period, because we want the SDK's own timeout enforcement to kick in first, so it can
   * throw the right kind of exception, etc.
   * <p>
   * There's no call timeout, because the Netty implementation didn't enforce one, and a streamed
   * response switches to a shorter read timeout once the request completes.
   */
  private static Duration socketReadTimeout(Duration requestTimeout) {
    return atLeastOneMillisecond(requestTimeout.plus(READ_TIMEOUT_GRACE_PERIOD));
  }

  /**
   * Changes the read timeout of the socket the request was sent on, for the rest of its response.
   * For example, once a streamed response has started, to detect a dead connection sooner.
   * <p>
   * Must be called on the thread reading the response.
   */
  static void setSocketReadTimeout(Request request, Duration timeout) {
    DispatchState s = request.tag(DispatchState.class);
    Socket socket = s == null ? null : s.socket;
    if (socket == null) {
      log.warn("Can't set the socket read timeout; the request has no socket. This is a bug.");
      return;
    }
    try {
      socket.setSoTimeout(toMillisInt(atLeastOneMillisecond(timeout)));
    } catch (SocketException e) {
      // The socket is closed, so the next read fails anyway.
      log.debug("Failed to set the socket read timeout.", e);
    }
  }

  private static int toMillisInt(Duration d) {
    return (int) Math.min(Integer.MAX_VALUE, d.toMillis());
  }

  @Override
  public void close() {
    client.dispatcher().executorService().shutdown();
    client.dispatcher().cancelAll();
    client.connectionPool().evictAll();
  }
}
