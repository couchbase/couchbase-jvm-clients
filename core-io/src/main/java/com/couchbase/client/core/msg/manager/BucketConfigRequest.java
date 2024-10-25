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

package com.couchbase.client.core.msg.manager;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.DefaultFullHttpRequest;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.FullHttpRequest;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpMethod;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpResponse;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpVersion;
import com.couchbase.client.core.endpoint.http.CoreHttpPath;
import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.msg.TargetedRequest;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.topology.NodeIdentifier;

import java.time.Duration;
import java.util.Map;
import java.util.TreeMap;

import static com.couchbase.client.core.io.netty.HttpProtocol.decodeStatus;
import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static com.couchbase.client.core.logging.RedactableArgument.redactSystem;

public class BucketConfigRequest extends BaseManagerRequest<BucketConfigResponse> implements TargetedRequest {

  private static final String PATH = "/pools/default/b/{}";

  private final String bucketName;
  private final Authenticator authenticator;
  private final NodeIdentifier target;

  public BucketConfigRequest(Duration timeout, CoreContext ctx, RetryStrategy retryStrategy,
                             String bucketName, Authenticator authenticator, final NodeIdentifier target) {
    super(timeout, ctx, retryStrategy);
    this.bucketName = bucketName;
    this.authenticator = authenticator;
    this.target = target;
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
  public NodeIdentifier target() {
    return target;
  }

  @Override
  public BucketConfigResponse decode(final HttpResponse response, final byte[] content) {
    return new BucketConfigResponse(decodeStatus(response.status()), content);
  }

  @Override
  public boolean idempotent() {
    return true;
  }

  @Override
  public Map<String, Object> serviceContext() {
    final Map<String, Object> ctx = new TreeMap<>();
    ctx.put("type", serviceType().ident());
    ctx.put("bucket", redactMeta(bucketName));
    if (target != null) {
      ctx.put("target", redactSystem(target));
    }
    return ctx;
  }

}
