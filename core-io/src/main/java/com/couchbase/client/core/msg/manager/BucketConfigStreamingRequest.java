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

package com.couchbase.client.core.msg.manager;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.DefaultFullHttpRequest;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.FullHttpRequest;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpMethod;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpResponse;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpVersion;
import com.couchbase.client.core.endpoint.http.CoreHttpPath;
import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.io.netty.HttpProtocol;
import com.couchbase.client.core.retry.RetryStrategy;

import java.time.Duration;

/**
 * Performs a (potential endless) streaming request against the cluster manager for the given bucket.
 */
public class BucketConfigStreamingRequest extends BaseManagerRequest<BucketConfigStreamingResponse> {

  private static final String PATH = "/pools/default/bs/{}";

  private final String bucketName;
  private final Authenticator authenticator;

  public BucketConfigStreamingRequest(final Duration timeout, final CoreContext ctx,
                                      final RetryStrategy retryStrategy, final String bucketName,
                                      final Authenticator authenticator) {
    super(timeout, ctx, retryStrategy);
    this.bucketName = bucketName;
    this.authenticator = authenticator;
  }

  @Override
  public BucketConfigStreamingResponse decode(final HttpResponse response, final byte[] content) {
    String lastDispatchedTo = null;
    if (context().lastDispatchedTo() != null) {
      lastDispatchedTo = context().lastDispatchedTo().host();
    }
    return new BucketConfigStreamingResponse(HttpProtocol.decodeStatus(response.status()), lastDispatchedTo);
  }

  @Override
  public FullHttpRequest encode() {
    FullHttpRequest request = new DefaultFullHttpRequest(
      HttpVersion.HTTP_1_1,
      HttpMethod.GET,
      CoreHttpPath.formatPath(PATH, bucketName)
    );
    authenticator.authHttpRequest(serviceType(), request);
    return request;
  }

  @Override
  public boolean idempotent() {
    return true;
  }

}
