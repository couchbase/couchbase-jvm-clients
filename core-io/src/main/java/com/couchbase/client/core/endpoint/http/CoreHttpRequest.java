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

import com.couchbase.client.core.Core;
import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.CbTracing;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufUtil;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.DefaultFullHttpRequest;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.DefaultHttpHeaders;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.EmptyHttpHeaders;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.FullHttpRequest;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.FullHttpResponse;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpHeaderNames;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpHeaderValues;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpHeaders;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpMethod;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpVersion;
import com.couchbase.client.core.error.HttpStatusCodeException;
import com.couchbase.client.core.io.netty.HttpChannelContext;
import com.couchbase.client.core.msg.BaseRequest;
import com.couchbase.client.core.msg.NonChunkedHttpRequest;
import com.couchbase.client.core.msg.RequestTarget;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.topology.NodeIdentifier;
import com.couchbase.client.core.util.UrlQueryStringBuilder;
import reactor.util.annotation.Nullable;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpMethod.GET;
import static com.couchbase.client.core.endpoint.http.CoreHttpTimeoutHelper.resolveTimeout;
import static com.couchbase.client.core.io.netty.HttpProtocol.decodeStatus;
import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static com.couchbase.client.core.logging.RedactableArgument.redactSystem;
import static com.couchbase.client.core.util.CbObjects.defaultIfNull;
import static java.util.Objects.requireNonNull;

@Stability.Internal
public class CoreHttpRequest extends BaseRequest<CoreHttpResponse>
    implements NonChunkedHttpRequest<CoreHttpResponse> {

  private final RequestTarget target;
  private final HttpMethod method;
  private final CoreHttpPath path;
  private final String queryString;
  private final ByteBuf content;
  private final HttpHeaders headers;
  private final boolean idempotent;
  private final AtomicBoolean executed = new AtomicBoolean();
  private final boolean bypassExceptionTranslation;
  private final @Nullable String name;

  public static Builder builder(CoreCommonOptions options, CoreContext coreContext, HttpMethod method, CoreHttpPath path, RequestTarget target) {
    return new Builder(options, coreContext, target, method, path);
  }

  private CoreHttpRequest(Builder builder, RequestSpan span, Duration timeout, RetryStrategy retry, @Nullable String name) {
    super(timeout, builder.coreContext, retry, span);

    this.target = builder.target;
    this.method = builder.method;
    this.path = builder.path;
    this.queryString = builder.queryString;
    this.content = builder.content;
    this.headers = builder.headers;
    this.idempotent = defaultIfNull(builder.idempotent, method == GET);
    this.bypassExceptionTranslation = builder.bypassExceptionTranslation;
    this.name = name;

    if (span != null && !CbTracing.isInternalSpan(span)) {
      span.lowCardinalityAttribute(TracingIdentifiers.ATTR_SERVICE, CbTracing.getTracingId(target.serviceType()));
      span.attribute(TracingIdentifiers.ATTR_OPERATION, builder.method + " " + builder.path.format());
    }
  }

  @Override
  public String name() {
    return name == null ? super.name() : name;
  }

  public CompletableFuture<CoreHttpResponse> exec(Core core) {
    if (!executed.compareAndSet(false, true)) {
      throw new IllegalStateException("This method may only be called once.");
    }
    core.send(this);
    return response()
        .whenComplete((r, t) -> context().logicallyComplete());
  }

  @Override
  public FullHttpRequest encode() {
    FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, pathAndQueryString(), content);
    request.headers().add(headers);
    context().authenticator().authHttpRequest(serviceType(), request);
    return request;
  }

  @Override
  public CoreHttpResponse decode(FullHttpResponse response, HttpChannelContext channelContext) {
    byte[] content = ByteBufUtil.getBytes(response.content());
    return new CoreHttpResponse(decodeStatus(response.status()), content, response.status().code(), channelContext, context());
  }

  @Override
  public ServiceType serviceType() {
    return target.serviceType();
  }

  @Override
  public boolean idempotent() {
    return idempotent;
  }

  /**
   * @return (nullable) node the request should be dispatched to,
   * or null to let the service locator decide.
   */
  public NodeIdentifier target() {
    return target.nodeIdentifier();
  }

  /**
   * @return (nullable) name of the bucket this request is scoped to, or null if not scoped to a bucket.
   */
  public String bucket() {
    return target.bucketName();
  }

  @Override
  public Map<String, Object> serviceContext() {
    Map<String, Object> ctx = new TreeMap<>();
    ctx.put("type", serviceType().ident());
    ctx.put("method", method.toString());
    ctx.put("path", redactMeta(pathAndQueryString()));
    if (target() != null) {
      ctx.put("target", redactSystem(target()));
    }
    if (bucket() != null) {
      ctx.put("bucket", redactMeta(bucket()));
    }
    return ctx;
  }

  private String pathAndQueryString() {
    String p = path.format();
    if (queryString.isEmpty()) {
      return p;
    }
    // If user was cheeky and included query string in path, join with "&" instead of "?"
    String delimiter = p.contains("?") ? "&" : "?";
    return p + delimiter + queryString;
  }

  @Override
  public String toString() {
    return "CoreHttpRequest{" +
        "serviceContext=" + serviceContext() +
        ",headers=" + headers +
        '}';
  }

  @Override
  public boolean bypassExceptionTranslation() {
    return bypassExceptionTranslation;
  }

  /**
   * Builder for dynamic HTTP requests
   */
  @Stability.Internal
  public static class Builder {
    private static final ByteBuf EMPTY_BYTE_BUF = Unpooled.buffer(0, 0);

    private final CoreCommonOptions options;
    private final CoreContext coreContext;
    private final RequestTarget target;
    private final HttpMethod method;
    private final CoreHttpPath path;

    private String queryString = "";
    private ByteBuf content = EMPTY_BYTE_BUF;

    private HttpHeaders headers = EmptyHttpHeaders.INSTANCE;
    private String spanName; // nullable
    @Nullable private Consumer<RequestSpan> attributeSetter;
    private Map<String, Object> spanAttributes; // nullable
    private Boolean idempotent; // nullable
    private boolean bypassExceptionTranslation;

    public Builder(CoreCommonOptions options, CoreContext coreContext, RequestTarget target, HttpMethod method, CoreHttpPath path) {
      this.options = requireNonNull(options);
      this.coreContext = requireNonNull(coreContext);
      this.target = requireNonNull(target);
      this.method = requireNonNull(method);
      this.path = requireNonNull(path);
    }

    public static UrlQueryStringBuilder newForm() {
      return UrlQueryStringBuilder.createForUrlSafeNames();
    }

    public static UrlQueryStringBuilder newQueryString() {
      return UrlQueryStringBuilder.createForUrlSafeNames();
    }

    /**
     * @param spanName (nullable)
     */
    public Builder trace(String spanName) {
      this.spanName = spanName;
      return this;
    }

    public Builder trace(
      @Nullable String spanName,
      @Nullable Consumer<RequestSpan> attributeSetter
    ) {
      this.attributeSetter = attributeSetter;
      return trace(spanName);
    }

    public Builder traceAttr(String attributeName, Object attributeValue) {
      if (this.spanAttributes == null) {
        this.spanAttributes = new HashMap<>();
      }
      spanAttributes.put(attributeName, requireNonNull(attributeValue));
      return this;
    }

    public Builder traceBucket(String bucketName) {
      return traceAttr(TracingIdentifiers.ATTR_NAME, bucketName);
    }

    public Builder traceScope(String scopeName) {
      return traceAttr(TracingIdentifiers.ATTR_SCOPE, scopeName);
    }

    public Builder traceCollection(String collectionName) {
      return traceAttr(TracingIdentifiers.ATTR_COLLECTION, collectionName);
    }

    public Builder header(CharSequence name, Object value) {
      if (this.headers == EmptyHttpHeaders.INSTANCE) {
        this.headers = new DefaultHttpHeaders();
      }
      this.headers.add(name, value);
      return this;
    }

    public Builder idempotent(boolean idempotent) {
      this.idempotent = idempotent;
      return this;
    }

    public Builder queryString(UrlQueryStringBuilder queryString) {
      return queryString(queryString.build());
    }

    public Builder queryString(String preEncodedQueryString) {
      this.queryString = requireNonNull(preEncodedQueryString);
      return this;
    }

    /**
     * If true, a non-2xx HTTP status codes is always reported as an {@link HttpStatusCodeException}.
     * If false, the message handler may throw a domain-specific exception instead.
     * <p>
     * Defaults to false.
     *
     * @see NonChunkedHttpRequest#bypassExceptionTranslation
     */
    public Builder bypassExceptionTranslation(boolean bypass) {
      this.bypassExceptionTranslation = bypass;
      return this;
    }

    public Builder form(UrlQueryStringBuilder formData) {
      return content(
          formData.build().getBytes(StandardCharsets.UTF_8),
          HttpHeaderValues.APPLICATION_X_WWW_FORM_URLENCODED
      );
    }

    public Builder json(byte[] content) {
      return content(content, HttpHeaderValues.APPLICATION_JSON);
    }

    public Builder content(byte[] content, CharSequence contentType) {
      this.content = Unpooled.wrappedBuffer(content);
      return header(HttpHeaderNames.CONTENT_TYPE, contentType)
          .header(HttpHeaderNames.CONTENT_LENGTH, content.length);
    }

    public CoreHttpRequest build() {
      RequestSpan span = spanName == null ? null : coreContext.coreResources().requestTracer().requestSpan(spanName, options.parentSpan().orElse(null));

      if (span != null && !CbTracing.isInternalSpan(span)) {
        if (target.bucketName() != null) {
          span.lowCardinalityAttribute(TracingIdentifiers.ATTR_NAME, target.bucketName());
        }
        CbTracing.setAttributes(span, spanAttributes);
        if (attributeSetter != null) {
          attributeSetter.accept(span);
        }
      }

      return new CoreHttpRequest(
          this,
          span,
          resolveTimeout(coreContext, target.serviceType(), options.timeout()),
          options.retryStrategy().orElse(null),
          spanName
      );
    }

    public CompletableFuture<CoreHttpResponse> exec(Core core) {
      return build().exec(core);
    }
  }
}
