package com.couchbase.client.core.msg.query;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.env.Credentials;
import com.couchbase.client.core.msg.BaseRequest;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.service.ServiceType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.base64.Base64;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.CharsetUtil;

import java.time.Duration;

public class QueryRequest extends BaseRequest<QueryResponse> {

  private static final String URI = "/query";

  private final byte[] query;
  final QueryResponse.QueryEventSubscriber subscriber;
  private final Credentials credentials;

  public QueryRequest(final Duration timeout, final CoreContext ctx,
                      final RetryStrategy retryStrategy, Credentials credentials, final byte[] query,
                      final QueryResponse.QueryEventSubscriber subscriber) {
    super(timeout, ctx, retryStrategy);
    this.query = query;
    this.subscriber = subscriber;
    this.credentials = credentials;
  }

  public QueryResponse.QueryEventSubscriber subscriber() {
    return subscriber;
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
    request.headers().set(HttpHeaderNames.USER_AGENT, context().environment().userAgent().formattedLong());
    addHttpBasicAuth(request, credentials.usernameForBucket(""), credentials.passwordForBucket(""));
    return request;
  }

  public static void addHttpBasicAuth(final HttpRequest request, final String user,
                                      final String password) {

    // if both user and password are null or empty, don't add http basic auth
    // this is usually the case when certificate auth is used.
    if ((user == null || user.isEmpty()) && (password == null || password.isEmpty())) {
      return;
    }

    final String pw = password == null ? "" : password;

    ByteBuf raw = Unpooled.buffer(user.length() + pw.length() + 1);
    raw.writeBytes((user + ":" + pw).getBytes(CharsetUtil.UTF_8));
    ByteBuf encoded = Base64.encode(raw, false);
    request.headers().add(HttpHeaders.Names.AUTHORIZATION, "Basic " + encoded.toString(CharsetUtil.UTF_8));
    encoded.release();
    raw.release();
  }

}
