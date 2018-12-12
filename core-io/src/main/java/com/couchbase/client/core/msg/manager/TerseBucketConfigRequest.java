package com.couchbase.client.core.msg.manager;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.env.Credentials;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.retry.RetryStrategy;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.base64.Base64;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;

import java.time.Duration;

public class TerseBucketConfigRequest extends BaseManagerRequest<TerseBucketConfigResponse> {

  private static final String TERSE_URI = "/pools/default/b/%s";

  private final String bucketName;
  private final Credentials credentials;

  public TerseBucketConfigRequest(Duration timeout, CoreContext ctx, RetryStrategy retryStrategy,
                                  String bucketName, Credentials credentials) {
    super(timeout, ctx, retryStrategy);
    this.bucketName = bucketName;
    this.credentials = credentials;
  }

  @Override
  public FullHttpRequest encode() {
    FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, String.format(TERSE_URI, bucketName));
    addHttpBasicAuth(request, credentials.usernameForBucket(bucketName), credentials.passwordForBucket(bucketName));
    return request;
  }

  @Override
  public TerseBucketConfigResponse decode(byte[] content) {
    return new TerseBucketConfigResponse(ResponseStatus.SUCCESS, content);
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
