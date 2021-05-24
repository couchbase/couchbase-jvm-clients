/*
 * Copyright 2021 Couchbase, Inc.
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

package com.couchbase.client.core.endpoint.http;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.io.netty.HttpChannelContext;
import com.couchbase.client.core.msg.BaseResponse;
import com.couchbase.client.core.msg.RequestContext;
import com.couchbase.client.core.msg.ResponseStatus;

import static com.couchbase.client.core.logging.RedactableArgument.redactUser;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

@Stability.Internal
public class CoreHttpResponse extends BaseResponse {
  private final int httpStatus;
  private final byte[] content;
  private final HttpChannelContext channelContext;
  private final RequestContext requestContext;

  public CoreHttpResponse(ResponseStatus status, byte[] content, int httpStatus, HttpChannelContext channelContext, RequestContext requestContext) {
    super(requireNonNull(status));
    this.httpStatus = httpStatus;
    this.content = requireNonNull(content);
    this.channelContext = requireNonNull(channelContext);
    this.requestContext = requireNonNull(requestContext);
  }

  public int httpStatus() {
    return httpStatus;
  }

  public byte[] content() {
    return content;
  }

  public HttpChannelContext channelContext() {
    return channelContext;
  }

  public String channelId() {
    return channelContext.channelId().asShortText();
  }

  public RequestContext requestContext() {
    return requestContext;
  }

  @Override
  public String toString() {
    return "CoreHttpResponse{" +
        "status=" + status() +
        ", httpStatus=" + httpStatus +
        ", content=" + redactUser(new String(content, UTF_8)) +
        '}';
  }
}
