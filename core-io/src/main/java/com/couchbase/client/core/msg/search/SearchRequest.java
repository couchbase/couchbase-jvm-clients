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
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.*;
import com.couchbase.client.core.env.Credentials;
import com.couchbase.client.core.msg.BaseRequest;
import com.couchbase.client.core.msg.HttpRequest;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.service.ServiceType;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

import static com.couchbase.client.core.io.netty.HttpProtocol.addHttpBasicAuth;

public class SearchRequest extends BaseRequest<SearchResponse>
        implements HttpRequest<SearchChunkHeader, SearchChunkRow, SearchChunkTrailer, SearchResponse> {

    private final String indexName;
    private final byte[] content;
    private final Credentials credentials;

    // TODO expose all other parameters
    public SearchRequest(Duration timeout,
                         CoreContext ctx,
                         RetryStrategy retryStrategy,
                         Credentials credentials,
                         String indexName,
                         byte[] content) {
        super(timeout, ctx, retryStrategy);
        this.indexName = indexName;
        this.content = content;
        this.credentials = credentials;
    }

    @Override
    public FullHttpRequest encode() {
        ByteBuf c = Unpooled.wrappedBuffer(content);
        String uri = "/api/index/" + indexName + "/query";
        FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, uri, c);
        request.headers().set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON);
        request.headers().set(HttpHeaderNames.CONTENT_LENGTH, c.readableBytes());
        addHttpBasicAuth(request, credentials.usernameForBucket(""), credentials.passwordForBucket(""));
        return request;
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
}
