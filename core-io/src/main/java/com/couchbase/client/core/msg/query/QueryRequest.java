package com.couchbase.client.core.msg.query;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.msg.BaseRequest;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.service.ServiceType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpVersion;

import java.time.Duration;
import java.util.function.Consumer;

public class QueryRequest extends BaseRequest<QueryResponse> {

  private static final String URI = "/query";

  private final byte[] query;
  final Consumer<QueryResponse.Row> consumer;

  public QueryRequest(final Duration timeout, final CoreContext ctx,
                      final RetryStrategy retryStrategy, final byte[] query,
                      final Consumer<QueryResponse.Row> consumer) {
    super(timeout, ctx, retryStrategy);
    this.query = query;
    this.consumer = consumer;
  }

  public Consumer<QueryResponse.Row> consumer() {
    return consumer;
  }

  @Override
  public ServiceType serviceType() {
    return ServiceType.QUERY;
  }

  public FullHttpRequest encode() {
    ByteBuf content = Unpooled.wrappedBuffer(query);
    FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST,
      URI, content);
    request.headers().set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON);
    request.headers().set(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes());
    return request;
  }

}
