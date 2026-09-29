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
import com.couchbase.client.core.env.SecurityConfig;
import com.couchbase.client.core.util.CbThreads;
import okhttp3.Call;
import okhttp3.ConnectionPool;
import okhttp3.Dispatcher;
import okhttp3.EventListener;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import org.jspecify.annotations.NullMarked;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static com.couchbase.client.core.service.OkHttpTlsHelper.configureTls;
import static java.util.Objects.requireNonNull;

@NullMarked
@Stability.Internal
public class CouchbaseOkHttpClient implements Closeable {
  private static final Logger log = LoggerFactory.getLogger(CouchbaseOkHttpClient.class);

  private static class DispatchState {
    volatile boolean requestStarted;
  }

  static boolean requestStarted(Request request) {
    DispatchState dispatchState = request.tag(DispatchState.class);
    if (dispatchState == null) {
      log.warn("dispatch state is null; this is a bug.", new RuntimeException("missing dispatch state tag"));
      return true; // assume the worst
    }
    return dispatchState.requestStarted;
  }

  public static okhttp3.Request.Builder newRequestBuilder() {
    return new okhttp3.Request.Builder()
      .tag(DispatchState.class, new DispatchState());
  }

  final OkHttpClient client;
  private final Duration defaultTimeout;

  public static final Duration QUERY_TIMEOUT_GRACE_PERIOD = Duration.ofSeconds(15);

  public CouchbaseOkHttpClient(
    Duration connectTimeout,
    Duration defaultTimeout,
    SecurityConfig securityConfig,
    Authenticator credential,
    int maxRequestsPerHost,
    String userAgent
  ) {
    // Bump sub-millisecond timeouts up to one millisecond, otherwise OkHttp rounds down to zero (no timeout).
    connectTimeout = atLeastOneMillisecond(connectTimeout);
    defaultTimeout = atLeastOneMillisecond(defaultTimeout);

    this.defaultTimeout = requireNonNull(defaultTimeout);

    Dispatcher dispatcher = new Dispatcher(CbThreads.unboundedExecutorService("cb-okhttp-dispatcher-"));

    OkHttpClient.Builder clientBuilder = new OkHttpClient.Builder()
      .connectTimeout(connectTimeout)
      .dispatcher(dispatcher)

      // The server doesn't start sending a response until it's done executing the query.
      // Set the read timeout to the query timeout so OkHttp doesn't time out before the query timeout elapses.
      // Add a grace period because we want the SDK's own timeout enforcement to kick in sooner so it can
      // throw the right kind of exceptions, etc.
      .readTimeout(defaultTimeout.plus(QUERY_TIMEOUT_GRACE_PERIOD))

      // TODO configure connection pool using IoConfig?
      .connectionPool(new ConnectionPool(16, 1, TimeUnit.SECONDS))
      .addInterceptor(chain -> {
        okhttp3.Request.Builder requestBuilder = chain.request().newBuilder()
          .header("User-Agent", userAgent);

        // get the value every time in case the credential is dynamic (deprecated)
        String authorizationHeaderValue = credential.getAuthHeaderValue();
        if (authorizationHeaderValue != null) {
          requestBuilder.header("Authorization", authorizationHeaderValue);
        }

        return chain.proceed(requestBuilder.build());
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

    configureTls(
      clientBuilder,
      credential,
      securityConfig
    );

    this.client = clientBuilder.build();

    this.client.dispatcher().setMaxRequests(Integer.MAX_VALUE);
    this.client.dispatcher().setMaxRequestsPerHost(maxRequestsPerHost);
  }

  private static final Duration ONE_MILLISECOND = Duration.ofMillis(1);

  private static Duration atLeastOneMillisecond(Duration d) {
    return d.toMillis() > 0 ? d : ONE_MILLISECOND;
  }

  public OkHttpClient clientWithTimeout(Duration timeout) {
    if (timeout.equals(defaultTimeout)) {
      return client;
    }

    timeout = timeout.plus(QUERY_TIMEOUT_GRACE_PERIOD);

    // set floor at 1 millisecond, otherwise OkHttp rounds down to zero which disables the timeout.
    timeout = atLeastOneMillisecond(timeout);

    if (timeout.toMillis() < 1) {
      timeout = Duration.ofMillis(1);
    }

    return client.newBuilder()
      .readTimeout(timeout)
//      .callTimeout(timeout) // no call timeout because legacy impl didn't enforce one, and we switch to shorter read timeout after receiving header
      .build();
  }

  @Override
  public void close() {
    client.dispatcher().executorService().shutdown();
    client.dispatcher().cancelAll();
    client.connectionPool().evictAll();
  }
}
