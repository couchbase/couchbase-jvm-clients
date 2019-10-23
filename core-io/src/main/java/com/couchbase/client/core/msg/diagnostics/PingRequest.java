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

package com.couchbase.client.core.msg.diagnostics;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.DefaultFullHttpRequest;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.FullHttpRequest;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.FullHttpResponse;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpMethod;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpVersion;
import com.couchbase.client.core.msg.BaseRequest;
import com.couchbase.client.core.msg.NonChunkedHttpRequest;
import com.couchbase.client.core.msg.ScopedRequest;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.service.ServiceType;

import java.time.Duration;

import static com.couchbase.client.core.io.netty.HttpProtocol.decodeStatus;
import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static com.couchbase.client.core.logging.RedactableArgument.redactSystem;

public class PingRequest
  extends BaseRequest<PingResponse>
  implements NonChunkedHttpRequest<PingResponse>, ScopedRequest {

  private final String bucket;
  private final String path;
  private final ServiceType type;

  public PingRequest(final Duration timeout,
                     final CoreContext ctx,
                     final RetryStrategy retryStrategy,
                     final String bucket,
                     final String path,
                     final ServiceType type) {
    super(timeout, ctx, retryStrategy);
    this.bucket = bucket;
    this.path = path;
    this.type = type;
  }

  @Override
  public PingResponse decode(final FullHttpResponse response) {
    byte[] dst = new byte[response.content().readableBytes()];
    response.content().readBytes(dst);
    return new PingResponse(decodeStatus(response.status()), dst);
  }

  @Override
  public FullHttpRequest encode() {
    return new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, path);
  }

  @Override
  public ServiceType serviceType() {
    return type;
  }

  @Override
  public boolean idempotent() {
    return true;
  }

  @Override
  public String bucket() {
    return bucket;
  }

  @Override
  public String toString() {
    return "PingRequest{" +
      "bucket='" + redactMeta(bucket) + '\'' +
      ", path='" + path + '\'' +
      ", type=" + type +
      '}';
  }
}
