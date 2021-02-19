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

package com.couchbase.client.core.msg.view;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufUtil;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.DefaultFullHttpRequest;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.FullHttpRequest;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.FullHttpResponse;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpMethod;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpVersion;
import com.couchbase.client.core.io.netty.HttpChannelContext;
import com.couchbase.client.core.msg.BaseRequest;
import com.couchbase.client.core.msg.NonChunkedHttpRequest;
import com.couchbase.client.core.msg.ScopedRequest;
import com.couchbase.client.core.msg.TargetedRequest;
import com.couchbase.client.core.node.NodeIdentifier;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.service.ServiceType;

import java.time.Duration;
import java.util.Map;
import java.util.TreeMap;

import static com.couchbase.client.core.io.netty.HttpProtocol.decodeStatus;
import static com.couchbase.client.core.logging.RedactableArgument.redactSystem;

public class ViewPingRequest extends BaseRequest<ViewPingResponse>
  implements NonChunkedHttpRequest<ViewPingResponse>, TargetedRequest, ScopedRequest {

  private final NodeIdentifier target;
  private final String bucket;

  public ViewPingRequest(Duration timeout, CoreContext ctx, RetryStrategy retryStrategy, String bucket, NodeIdentifier target) {
    super(timeout, ctx, retryStrategy);
    this.target = target;
    this.bucket = bucket;
  }

  @Override
  public ViewPingResponse decode(final FullHttpResponse response, final HttpChannelContext context) {
    byte[] dst = ByteBufUtil.getBytes(response.content());
    return new ViewPingResponse(decodeStatus(response.status()), dst, context.channelId().asShortText());
  }

  @Override
  public FullHttpRequest encode() {
    return new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
  }

  @Override
  public String bucket() {
    return bucket;
  }

  @Override
  public ServiceType serviceType() {
    return ServiceType.VIEWS;
  }

  @Override
  public NodeIdentifier target() {
    return target;
  }

  @Override
  public boolean idempotent() {
    return true;
  }

  @Override
  public Map<String, Object> serviceContext() {
    final Map<String, Object> ctx = new TreeMap<>();
    ctx.put("type", serviceType().ident());
    ctx.put("bucket", bucket);
    if (target != null) {
      ctx.put("target", redactSystem(target.address()));
    }
    return ctx;
  }

}

