package com.couchbase.client.core.msg.search;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.*;
import com.couchbase.client.core.env.Credentials;
import com.couchbase.client.core.msg.BaseRequest;
import com.couchbase.client.core.msg.NonChunkedHttpRequest;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.util.ResponseStatusConverter;

import java.time.Duration;

import static com.couchbase.client.core.io.netty.HttpProtocol.addHttpBasicAuth;

public class UpsertSearchIndexRequest extends BaseRequest<UpsertSearchIndexResponse>
  implements NonChunkedHttpRequest<UpsertSearchIndexResponse> {

  private static final String PATH = "/api/index/";

  private final String name;
  private final Credentials credentials;
  private final byte[] payload;

  public UpsertSearchIndexRequest(Duration timeout, CoreContext ctx, RetryStrategy retryStrategy,
                                  Credentials credentials, String name, byte[] payload) {
    super(timeout, ctx, retryStrategy);
    this.name = name;
    this.credentials = credentials;
    this.payload = payload;
  }

  @Override
  public FullHttpRequest encode() {
    FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.PUT, PATH + name,
      Unpooled.wrappedBuffer(payload));
    addHttpBasicAuth(request, credentials.usernameForBucket(""), credentials.passwordForBucket(""));
    request.headers().set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON);
    request.headers().set(HttpHeaderNames.CONTENT_LENGTH, payload.length);
    return request;
  }

  @Override
  public UpsertSearchIndexResponse decode(final FullHttpResponse response) {
    byte[] dst = new byte[response.content().readableBytes()];
    response.content().readBytes(dst);
    return new UpsertSearchIndexResponse(ResponseStatusConverter.fromHttp(response.status().code()), dst);
  }

  @Override
  public ServiceType serviceType() {
    return ServiceType.SEARCH;
  }
}
