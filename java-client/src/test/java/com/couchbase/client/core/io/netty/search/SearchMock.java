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

package com.couchbase.client.core.io.netty.search;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.cnc.metrics.NoopMeter;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.deps.io.netty.channel.embedded.EmbeddedChannel;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.DefaultFullHttpRequest;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.DefaultHttpResponse;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.DefaultLastHttpContent;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpContent;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpMethod;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpResponse;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpResponseStatus;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpVersion;
import com.couchbase.client.core.deps.io.netty.util.ResourceLeakDetector;
import com.couchbase.client.core.endpoint.BaseEndpoint;
import com.couchbase.client.core.endpoint.EndpointContext;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.PasswordAuthenticator;
import com.couchbase.client.core.msg.search.SearchRequest;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import com.couchbase.client.core.util.HostAndPort;
import com.couchbase.client.java.codec.DefaultJsonSerializer;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.search.SearchAccessor;
import com.couchbase.client.java.search.result.SearchResult;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Mocks out the search code to allow doing unit tests against the search service.
 *
 * This makes it much easier to test against some hard-to-replicate issues.
 */
public class SearchMock {
    static {
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    }

    private static byte[] toByteArray(InputStream in) throws IOException {

        ByteArrayOutputStream os = new ByteArrayOutputStream();

        byte[] buffer = new byte[1024];
        int len;

        // read bytes from the input stream and store them in buffer
        while ((len = in.read(buffer)) != -1) {
            // write bytes from the buffer into output stream
            os.write(buffer, 0, len);
        }

        return os.toByteArray();
    }

    /**
     * Given JSON in the form expected, e.g. those from https://github.com/chvck/sdk-testcases which contains the
     * returned JSON from the search service in a field "data", returns the completed SearchResult that the API
     * would return.
     */
    public static SearchResult loadSearchTestCase(InputStream json) throws ExecutionException, InterruptedException, IOException {
        // The idea is to fake packets that have come from the search service.
        // Start by preparing the packets.
        JsonObject jo = JsonObject.fromJson(toByteArray(json));
        JsonObject data = jo.getObject("data");

        byte[] b = data.toString().getBytes(StandardCharsets.UTF_8);
        ByteBuf bytes = Unpooled.wrappedBuffer(b);

        HttpContent content = new DefaultLastHttpContent(bytes);
        HttpResponse resp = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);

        // Fake some core stuff
        Core mockedCore = mock(Core.class);
        CoreEnvironment env = CoreEnvironment.builder()
                .meter(NoopMeter.INSTANCE)
                .build();
        CoreContext ctx = new CoreContext(mockedCore, 0, env, PasswordAuthenticator.create("Administrator", "password"));

        // Our ChunkedSearchMessageHandler needs to be initialised by pretending we've sent an outbound SearchRequest
        // through it
        SearchRequest req = new SearchRequest(Duration.ofSeconds(10), ctx, BestEffortRetryStrategy.INSTANCE, null,
                null, null, null, null);

        // ChunkedSearchMessageHandler will try to encode() the SearchRequest.  Rather than mocking everything required
        // to get that working, just mock the encode method.
        SearchRequest spiedReq = spy(req);
        doReturn(new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "localhost")).when(spiedReq).encode();

        doAnswer(v -> {
            EndpointContext endpointContext = new EndpointContext(ctx, new HostAndPort("127.0.0.1", 0), null, null, null,
                    Optional.of("bucket"), null);

            BaseEndpoint endpoint = mock(BaseEndpoint.class);
            when(endpoint.context()).thenReturn(endpointContext);
            when(endpoint.pipelined()).thenReturn(false);

            // ChunkedSearchMessageHandler does most of the work in handling responses from the service
            ChunkedSearchMessageHandler handler = new ChunkedSearchMessageHandler(endpoint, endpointContext);

            // Netty's EmbeddedChannel lets us test ChannelHandlers like ChunkedSearchMessageHandler.  It's a Netty Channel
            // that doesn't touch the network at all.
            final EmbeddedChannel channel = new EmbeddedChannel(handler);

            // Writing the request is necessary to estabish some initial state inChunkedSearchMessageHandler
            channel.writeAndFlush(spiedReq);

            // Finally we can do the interesting bit of passing our fake FTS service response into
            // ChunkedSearchMessageHandler
            channel.writeInbound(resp);
            channel.writeInbound(content);

            return null;
        }).when(mockedCore).send(any());

        CompletableFuture<SearchResult> future = SearchAccessor.searchQueryAsync(mockedCore, req, DefaultJsonSerializer.create());
        SearchResult result = future.get();

        return result;
    }
}
