/*
 * Copyright 2019 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.java.manager;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.DefaultFullHttpRequest;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpHeaderValues;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpMethod;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpVersion;
import com.couchbase.client.core.error.BucketNotFlushableException;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.manager.GenericManagerRequest;
import com.couchbase.client.core.msg.manager.GenericManagerResponse;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.util.UrlQueryStringBuilder;
import com.couchbase.client.java.CommonOptions;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

@Stability.Internal
public abstract class ManagerSupport {

  final Core core;

  protected ManagerSupport(Core core) {
    this.core = requireNonNull(core);
  }

  protected CompletableFuture<GenericManagerResponse> sendRequest(GenericManagerRequest request) {
    core.send(request);
    return request.response();
  }

  protected CompletableFuture<GenericManagerResponse> sendRequest(HttpMethod method, String path, CommonOptions<?>.BuiltCommonOptions options) {
    Duration timeout = timeout(options);
    RetryStrategy retry = retryStrategy(options);

    return sendRequest(new GenericManagerRequest(timeout, core.context(), retry,
        () -> new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, path), method == HttpMethod.GET));
  }

  /**
   * @deprecated in favor of the version that takes an option block
   */
  @Deprecated
  protected CompletableFuture<GenericManagerResponse> sendRequest(HttpMethod method, String path) {
    return sendRequest(new GenericManagerRequest(core.context(),
        () -> new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, path), method == HttpMethod.GET));
  }

  protected CompletableFuture<GenericManagerResponse> sendRequest(HttpMethod method, String path, UrlQueryStringBuilder body, CommonOptions<?>.BuiltCommonOptions options) {
    Duration timeout = timeout(options);
    RetryStrategy retry = retryStrategy(options);

    return sendRequest(new GenericManagerRequest(timeout, core.context(), retry, () -> {
      ByteBuf content = Unpooled.copiedBuffer(body.build(), UTF_8);
      DefaultFullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, path, content);
      req.headers().add("Content-Type", HttpHeaderValues.APPLICATION_X_WWW_FORM_URLENCODED);
      req.headers().add("Content-Length", content.readableBytes());
      return req;
    }, method == HttpMethod.GET));
  }

  /**
   * @deprecated in favor of the version that takes an option block
   */
  @Deprecated
  protected CompletableFuture<GenericManagerResponse> sendRequest(HttpMethod method, String path, UrlQueryStringBuilder body) {
    return sendRequest(new GenericManagerRequest(core.context(), () -> {
      ByteBuf content = Unpooled.copiedBuffer(body.build(), UTF_8);
      DefaultFullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, path, content);
      req.headers().add("Content-Type", HttpHeaderValues.APPLICATION_X_WWW_FORM_URLENCODED);
      req.headers().add("Content-Length", content.readableBytes());
      return req;
    }, method == HttpMethod.GET));
  }

  protected static void checkStatus(GenericManagerResponse response, String action, String scope) {
    final String content = response.content() != null ? new String(response.content(), UTF_8) : "";

    if (response.status() == ResponseStatus.INVALID_ARGS && content.contains("Flush is disabled")) {
      throw BucketNotFlushableException.forBucket(scope);
    }
    if (response.status() != ResponseStatus.SUCCESS) {
      throw new CouchbaseException("Failed to " + action + "; response status=" + response.status() + "; response body=" + content);
    }
  }

  private Duration timeout(CommonOptions<?>.BuiltCommonOptions options) {
    return options.timeout().orElse(core.context().environment().timeoutConfig().managementTimeout());
  }

  private RetryStrategy retryStrategy(CommonOptions<?>.BuiltCommonOptions options) {
    return options.retryStrategy().orElse(core.context().environment().retryStrategy());
  }
}
