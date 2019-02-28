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

package com.couchbase.client.core.io.netty.query;

import static org.mockito.Mockito.*;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.Timer;
import com.couchbase.client.core.endpoint.EndpointContext;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.Credentials;
import com.couchbase.client.core.env.RoleBasedCredentials;
import com.couchbase.client.core.env.TimeoutConfig;
import com.couchbase.client.core.env.UserAgent;
import com.couchbase.client.core.error.QueryStreamException;
import com.couchbase.client.core.msg.query.QueryRequest;
import com.couchbase.client.core.msg.query.QueryResponse;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.CharsetUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

/**
 * Tests for query message handler
 */
public class QueryMessageHandlerTest {

    private static CoreEnvironment environment;
    private static EndpointContext endpointContext;
    private static CoreContext coreContext;
    private static Credentials credentials;

    @BeforeAll
    public static void setup() {
        environment = mock(CoreEnvironment.class);
        endpointContext = mock(EndpointContext.class);
        coreContext = mock(CoreContext.class);
        credentials = new RoleBasedCredentials("Administrator", "password");
        when(endpointContext.environment()).thenReturn(environment);
        when(coreContext.environment()).thenReturn(environment);
        when(environment.userAgent()).thenReturn(new UserAgent("sdk", "0", Optional.empty(), Optional.empty()));
        when(environment.timer()).thenReturn(Timer.create());
        when(environment.timeoutConfig()).thenReturn(TimeoutConfig.builder().queryStreamReleaseTimeout(1).build());
        when(coreContext.core()).thenReturn(mock(Core.class));
        when(coreContext.id()).thenReturn(0L);
    }

    @Test
    public void testChannelStreamTimeout() throws Exception {
        EmbeddedChannel embeddedChannel = new EmbeddedChannel(new QueryMessageHandler(endpointContext));
        String query = "select 1=1";
        QueryRequest queryRequest = new QueryRequest(Duration.ofSeconds(75), coreContext, BestEffortRetryStrategy.INSTANCE,
                credentials, query.getBytes());
        embeddedChannel.writeAndFlush(queryRequest);
        CompletableFuture.runAsync(() -> {
            HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
            httpResponse.headers().set(HttpHeaderNames.CONTENT_LENGTH, ("{\"results\": [{\"1\"},{\"2\"}]").length());
            embeddedChannel.writeInbound(httpResponse);
            embeddedChannel.writeInbound(new DefaultHttpContent(Unpooled.copiedBuffer("{\"results\": [{\"1\"},", CharsetUtil.UTF_8)));
            try {
                Thread.sleep(2000); //sleep a bit to simulate that it doesn't fail with Stream
            } catch(InterruptedException ex) {}
            embeddedChannel.writeInbound(new DefaultLastHttpContent(Unpooled.copiedBuffer("{\"2\"}]", CharsetUtil.UTF_8)));
            embeddedChannel.flushInbound();
        });

        QueryResponse response = queryRequest.response().get();
        StepVerifier.create(response.rows().map(String::new))
                .thenRequest(1)
                .consumeNextWith(n -> {
                    try {
                        Thread.sleep(1500);
                    } catch(Exception ex) {}
                })
                .verifyError(QueryStreamException.class);
    }

    @Test
    public void testChannelNoRequestException() throws Exception {
        EmbeddedChannel embeddedChannel = new EmbeddedChannel(new QueryMessageHandler(endpointContext));
        String query = "select 1=1";
        QueryRequest queryRequest = new QueryRequest(Duration.ofSeconds(75), coreContext, BestEffortRetryStrategy.INSTANCE,
                credentials, query.getBytes());
        embeddedChannel.writeAndFlush(queryRequest);
        CompletableFuture.runAsync(() -> {
            HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
            httpResponse.headers().set(HttpHeaderNames.CONTENT_LENGTH, ("{\"results\": [{\"3\"},{\"4\"}]").length());
            embeddedChannel.writeInbound(httpResponse);
            embeddedChannel.writeInbound(new DefaultHttpContent(Unpooled.copiedBuffer("{\"results\": [{\"3\"},", CharsetUtil.UTF_8)));
            embeddedChannel.writeInbound(new DefaultLastHttpContent(Unpooled.copiedBuffer("{\"4\"}]", CharsetUtil.UTF_8)));
            embeddedChannel.flushInbound();
        });

        QueryResponse response = queryRequest.response().get();
        StepVerifier.create(response.rows().map(String::new), 0)
                .verifyError(QueryStreamException.class);
    }
}