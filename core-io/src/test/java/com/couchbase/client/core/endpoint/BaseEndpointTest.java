/*
 * Copyright (c) 2018 Couchbase, Inc.
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

package com.couchbase.client.core.endpoint;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.cnc.Event;
import com.couchbase.client.core.cnc.events.endpoint.EndpointConnectAttemptFailedEvent;
import com.couchbase.client.core.cnc.events.endpoint.EndpointConnectedEvent;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.IoEnvironment;
import com.couchbase.client.core.io.NetworkAddress;
import com.couchbase.client.util.SimpleEventBus;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static com.couchbase.client.util.Utils.waitUntilCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class BaseEndpointTest {

  /**
   * Network address used for all the tests.
   */
  private static final NetworkAddress LOCALHOST = NetworkAddress.localhost();

  /**
   * The port used to connect.
   */
  private static final int PORT = 1234;

  private EventLoopGroup eventLoopGroup;
  private CoreContext ctx;

  @BeforeEach
  void beforeEach() {
    eventLoopGroup = new NioEventLoopGroup(1);
    CoreEnvironment env = CoreEnvironment.create();
    ctx = new CoreContext(1, env);
  }

  @AfterEach
  void afterEach() {
    eventLoopGroup.shutdownGracefully();
    ctx.environment().shutdown(Duration.ofSeconds(1));
  }

  @Test
  void startDisconnected() {
    InstrumentedEndpoint endpoint = InstrumentedEndpoint.create(eventLoopGroup, ctx, null);
    assertEquals(EndpointState.DISCONNECTED, endpoint.state());
  }

  @Test
  void connectSuccessfully() {
    final CompletableFuture<Channel> cf = new CompletableFuture<>();

    InstrumentedEndpoint endpoint = InstrumentedEndpoint.create(
      eventLoopGroup,
      ctx,
      () -> Mono.fromFuture(cf)
    );

    endpoint.connect();
    waitUntilCondition(() -> endpoint.state() == EndpointState.CONNECTING);

    EmbeddedChannel channel = new EmbeddedChannel();
    cf.complete(channel);
    waitUntilCondition(() -> endpoint.state() == EndpointState.CONNECTED_CIRCUIT_CLOSED);
  }

  @Test
  void retryOnTimeoutUntilEventuallyConnected() {
    SimpleEventBus eventBus = new SimpleEventBus();
    CoreEnvironment env = CoreEnvironment.builder()
      .eventBus(eventBus)
      .ioEnvironment(IoEnvironment.builder()
        .connectTimeout(Duration.ofMillis(10))
        .build())
      .build();
    CoreContext ctx = new CoreContext(1, env);

    try {
      final CompletableFuture<Channel> cf = new CompletableFuture<>();
      InstrumentedEndpoint endpoint = InstrumentedEndpoint.create(
        eventLoopGroup,
        ctx,
        () -> Mono.fromFuture(cf)
      );

      endpoint.connect();
      waitUntilCondition(() -> eventBus.publishedEvents().size() >= 3);

      EmbeddedChannel channel = new EmbeddedChannel();
      cf.complete(channel);
      waitUntilCondition(() -> endpoint.state() == EndpointState.CONNECTED_CIRCUIT_CLOSED);

      assertTrue(eventBus.publishedEvents().size() >= 3);
      boolean failedFound = false;
      boolean successFound = false;
      for (Event event : eventBus.publishedEvents()) {
        if (event instanceof EndpointConnectAttemptFailedEvent) {
          assertEquals(Event.Severity.WARN, event.severity());
          assertEquals(Duration.ofMillis(10), event.duration());
          failedFound = true;
        } else if (event instanceof EndpointConnectedEvent) {
          assertEquals(Event.Severity.DEBUG, event.severity());
          assertTrue(event.duration().toNanos() > 0);
          successFound = true;
          System.out.println(event.toString());
        } else {
          throw new IllegalStateException("Unexpected Event found!");
        }
      }
      assertTrue(failedFound);
      assertTrue(successFound);
    } finally {
      env.shutdown(Duration.ofSeconds(1));
    }
  }

  @Test
  void retryOnFailureUntilEventuallyConnected() {

  }

  @Test
  void disconnectOverridesConnectCompletion() {

  }

  @Test
  void disconnectDuringRetry() {

  }

  static class InstrumentedEndpoint extends BaseEndpoint {

    private Supplier<Mono<Channel>> channelSupplier;

    static InstrumentedEndpoint create(EventLoopGroup eventLoopGroup, CoreContext ctx,
                                       Supplier<Mono<Channel>> channelSupplier) {
      return new InstrumentedEndpoint(LOCALHOST, PORT, eventLoopGroup, ctx, channelSupplier);
    }

    InstrumentedEndpoint(NetworkAddress hostname, int port, EventLoopGroup eventLoopGroup,
                         CoreContext coreContext, Supplier<Mono<Channel>> channelSupplier) {
      super(hostname, port, eventLoopGroup, coreContext);
      this.channelSupplier = channelSupplier;
    }

    @Override
    protected PipelineInitializer pipelineInitializer() {
      return pipeline -> { };
    }

    @Override
    protected Mono<Channel> channelFutureIntoMono(ChannelFuture channelFuture) {
      if (channelSupplier == null) {
        return super.channelFutureIntoMono(channelFuture);
      } else {
        return channelSupplier.get();
      }
    }
  }
}