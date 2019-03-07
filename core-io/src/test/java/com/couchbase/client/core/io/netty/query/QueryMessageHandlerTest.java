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
import com.couchbase.client.core.deps.io.netty.util.ReferenceCountUtil;
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
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.deps.io.netty.channel.embedded.EmbeddedChannel;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.DefaultHttpContent;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.DefaultHttpResponse;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.DefaultLastHttpContent;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpHeaderNames;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpResponse;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpResponseStatus;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpVersion;
import com.couchbase.client.core.deps.io.netty.util.CharsetUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

/**
 * Tests for query message handler
 */
class QueryMessageHandlerTest {

    private static CoreEnvironment environment;
    private static EndpointContext endpointContext;
    private static CoreContext coreContext;
    private static Credentials credentials;

    @BeforeAll
    static void setup() {
        environment = mock(CoreEnvironment.class);
        endpointContext = mock(EndpointContext.class);
        coreContext = mock(CoreContext.class);
        credentials = new RoleBasedCredentials("Administrator", "password");
        when(endpointContext.environment()).thenReturn(environment);
        when(coreContext.environment()).thenReturn(environment);
        when(environment.userAgent()).thenReturn(
          new UserAgent("sdk", "0", Optional.empty(), Optional.empty())
        );
        when(environment.timer()).thenReturn(Timer.create());
        when(environment.timeoutConfig()).thenReturn(
          TimeoutConfig.builder().queryStreamReleaseTimeout(1).build()
        );
        when(coreContext.core()).thenReturn(mock(Core.class));
        when(coreContext.id()).thenReturn(0L);
    }

    @Test
    void testChannelStreamTimeout() throws Exception {
        EmbeddedChannel channel = new EmbeddedChannel(new QueryMessageHandler(endpointContext));
        String query = "select 1=1";
        QueryRequest queryRequest = new QueryRequest(Duration.ofSeconds(75), coreContext,
          BestEffortRetryStrategy.INSTANCE, credentials, query.getBytes());
        channel.writeAndFlush(queryRequest);
        ReferenceCountUtil.release(channel.readOutbound());

        CompletableFuture.runAsync(() -> {
            HttpResponse httpResponse =
              new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
            httpResponse.headers().set(
              HttpHeaderNames.CONTENT_LENGTH,
              "{\"results\": [{\"foo\"},{\"bar\"}]".length()
            );
            channel.writeInbound(httpResponse);
            channel.writeInbound(new DefaultHttpContent(
              Unpooled.copiedBuffer("{\"results\": [{\"foo\"},{\"", CharsetUtil.UTF_8)
            ));
            try {
                // sleep a bit to simulate that it doesn't fail with stream
                Thread.sleep(2000);
            } catch(InterruptedException ex) {}
            channel.writeInbound(new DefaultLastHttpContent(
              Unpooled.copiedBuffer("bar\"}]", CharsetUtil.UTF_8)
            ));
            channel.flushInbound();
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
    void testChannelNoRequestException() throws Exception {
        EmbeddedChannel channel = new EmbeddedChannel(
          new QueryMessageHandler(endpointContext)
        );

        String query = "select 1=1";
        QueryRequest queryRequest = new QueryRequest(Duration.ofSeconds(75), coreContext,
          BestEffortRetryStrategy.INSTANCE, credentials, query.getBytes());
        channel.writeAndFlush(queryRequest);
        ReferenceCountUtil.release(channel.readOutbound());

        CompletableFuture.runAsync(() -> {
            HttpResponse httpResponse =
              new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
            httpResponse.headers().set(
              HttpHeaderNames.CONTENT_LENGTH,
              "{\"results\": [{\"foo\"},{\"bar\"}]".length()
            );
            channel.writeInbound(httpResponse);
            channel.writeInbound(new DefaultHttpContent(
              Unpooled.copiedBuffer("{\"results\": [{\"foo\"},", CharsetUtil.UTF_8)
            ));
            channel.writeInbound(new DefaultLastHttpContent(
              Unpooled.copiedBuffer("{\"bar\"}]", CharsetUtil.UTF_8)
            ));
            channel.flushInbound();
        });

        QueryResponse response = queryRequest.response().get();
        StepVerifier
          .create(response.rows().map(String::new), 0)
          .verifyError(QueryStreamException.class);
    }
}