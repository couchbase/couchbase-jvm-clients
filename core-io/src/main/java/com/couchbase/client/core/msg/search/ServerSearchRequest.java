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

package com.couchbase.client.core.msg.search;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.manager.CoreBucketAndScope;
import com.couchbase.client.core.cnc.CbTracing;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.tracing.TracingAttribute;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.cnc.tracing.TracingDecorator;
import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.msg.BaseHttpRequest;
import com.couchbase.client.core.msg.HttpRequest;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.service.ServiceType;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.Map;
import java.util.TreeMap;

import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;

// This would generally be called just "SearchRequest", but that's now an externally presented concept in the API.
public class ServerSearchRequest extends BaseHttpRequest<SearchResponse>
        implements HttpRequest<SearchChunkHeader, SearchChunkRow, SearchChunkTrailer, SearchResponse> {

    private final String indexName;
    private final byte[] content;
    private final CoreBucketAndScope scope;

    public ServerSearchRequest(Duration timeout, CoreContext ctx, RetryStrategy retryStrategy, Authenticator authenticator,
                               String indexName, byte[] content, final RequestSpan span, @Nullable CoreBucketAndScope scope) {
        super(timeout, ctx, retryStrategy, span);
        this.indexName = indexName;
        this.content = content;
        this.scope = scope;

        if (span != null && !CbTracing.isInternalSpan(span)) {
          TracingDecorator tip = ctx.coreResources().tracingDecorator();
          tip.provideLowCardinalityAttr(TracingAttribute.SERVICE, span, TracingIdentifiers.SERVICE_SEARCH);
          tip.provideLowCardinalityAttr(TracingAttribute.OPERATION, span, indexName);
        }
    }

    @Override
    public ServiceType serviceType() {
        return ServiceType.SEARCH;
    }

    @Override
    public SearchResponse decode(ResponseStatus status, SearchChunkHeader header, Flux<SearchChunkRow> rows,
                                 Mono<SearchChunkTrailer> trailer) {
        return new SearchResponse(status, header, rows, trailer);
    }

    @Override
    public boolean idempotent() {
        return true;
    }

    @Override
    public Map<String, Object> serviceContext() {
        Map<String, Object> ctx = new TreeMap<>();
        ctx.put("type", serviceType().ident());
        ctx.put("indexName", redactMeta(indexName));
        return ctx;
    }

    @Override
    public String name() {
        return "search";
    }

    @Stability.Internal
    public String indexName() {
      return indexName;
    }

    @Stability.Internal
    public byte[] content() {
      return content;
    }

    public CoreBucketAndScope scope() {
      return scope;
    }
}
