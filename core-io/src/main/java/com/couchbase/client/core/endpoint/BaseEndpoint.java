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
import com.couchbase.client.core.cnc.events.endpoint.EndpointConnectionAbortedEvent;
import com.couchbase.client.core.cnc.events.endpoint.EndpointConnectionFailedEvent;
import com.couchbase.client.core.cnc.events.endpoint.EndpointConnectedEvent;
import com.couchbase.client.core.cnc.events.endpoint.EndpointConnectionIgnoredEvent;
import com.couchbase.client.core.cnc.events.endpoint.EndpointDisconnectedEvent;
import com.couchbase.client.core.cnc.events.endpoint.EndpointDisconnectionFailedEvent;
import com.couchbase.client.core.cnc.events.endpoint.UnexpectedEndpointConnectionFailedEvent;
import com.couchbase.client.core.io.NetworkAddress;
import com.couchbase.client.core.io.netty.kv.ConnectTimings;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.retry.RetryOrchestrator;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.kqueue.KQueueSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import reactor.core.publisher.Mono;
import reactor.retry.Retry;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * This {@link BaseEndpoint} implements all common logic for endpoints that wrap the IO layer.
 *
 * <p>In addition to just wrapping a netty channel, this implementaiton is also a circuit breaker
 * which is configurable and then determines base on the config if the circuit should be open
 * or closed. Half-Open states will allow canaries to go in and eventually open it again if they
 * are deemed okay.</p>
 *
 * @since 2.0.0
 */
public abstract class BaseEndpoint implements Endpoint {

  /**
   * Holds the current state of this endpoint.
   */
  private final AtomicReference<EndpointState> state;

  /**
   * The related context to use.
   */
  private final EndpointContext endpointContext;

  /**
   * Stores the common bootstrap logic for a channel.
   */
  private final Bootstrap channelBootstrap;

  /**
   * If instructed to disconnect, disrupts any connecting attempts
   * and shuts down the underlying channel if any.
   */
  private final AtomicBoolean disconnect;

  /**
   * The circuit breaker used for this endpoint.
   */
  private final CircuitBreaker circuitBreaker;

  /**
   * If the current endpoint is free or not.
   */
  private final AtomicInteger outstandingRequests;

  /**
   * Once connected, contains the channel to work with.
   */
  private volatile Channel channel;

  /**
   * Holds the unix nanotime when the last response completed.
   */
  private volatile long lastResponseTimestamp;

  /**
   * Constructor to create a new endpoint, usually called by subclasses.
   *
   * @param hostname the remote hostname.
   * @param port the remote port.
   * @param eventLoopGroup the netty event loop group to use.
   * @param coreContext the core context.
   * @param circuitBreakerConfig the circuit breaker config used.
   */
  BaseEndpoint(final NetworkAddress hostname, final int port, final EventLoopGroup eventLoopGroup,
               final CoreContext coreContext, final CircuitBreakerConfig circuitBreakerConfig) {
    this.state = new AtomicReference<>(EndpointState.DISCONNECTED);
    disconnect = new AtomicBoolean(false);
    this.circuitBreaker = circuitBreakerConfig.enabled()
      ? new LazyCircuitBreaker(circuitBreakerConfig)
      : NoopCircuitBreaker.INSTANCE;
    this.endpointContext = new EndpointContext(coreContext, hostname, port, circuitBreaker);
    this.outstandingRequests = new AtomicInteger(0);
    this.lastResponseTimestamp = 0;

    long connectTimeoutMs = coreContext.environment().ioEnvironment().connectTimeout().toMillis();
    channelBootstrap = new Bootstrap()
      .remoteAddress(hostname.nameOrAddress(), port)
      .group(eventLoopGroup)
      .channel(channelFrom(eventLoopGroup))
      .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) connectTimeoutMs)
      .handler(new ChannelInitializer<SocketChannel>() {
        @Override
        protected void initChannel(SocketChannel ch) {
          ChannelPipeline pipeline = ch.pipeline();
          pipelineInitializer().init(pipeline);
        }
      });
  }

  /**
   * Helper method to locate the right socket channel class based on the injected
   * event loop group.
   *
   * @param eventLoopGroup the group to compare against.
   * @return the channel class selected.
   */
  private static Class<? extends Channel> channelFrom(final EventLoopGroup eventLoopGroup) {
    if (eventLoopGroup instanceof KQueueEventLoopGroup) {
      return KQueueSocketChannel.class;
    } else if (eventLoopGroup instanceof EpollEventLoopGroup) {
      return EpollSocketChannel.class;
    } else if (eventLoopGroup instanceof NioEventLoopGroup) {
      return NioSocketChannel.class;
    } else {
      throw new IllegalArgumentException("Unknown EventLoopGroup Type: "
        + eventLoopGroup.getClass().getSimpleName());
    }
  }

  /**
   * Returns the initialize which adds endpoint-specific handlers to the pipeline.
   */
  protected abstract PipelineInitializer pipelineInitializer();

  /**
   * Starts the connect process of this endpoint.
   *
   * <p>Note that if the current state is not {@link EndpointState#DISCONNECTED}, this method
   * will do nothing.</p>
   */
  @Override
  public void connect() {
    if (state.compareAndSet(EndpointState.DISCONNECTED, EndpointState.CONNECTING)) {
      reconnect();
    }
  }

  /**
   * This method performs the actual connecting logic.
   *
   * <p>It is called reconnect since it works both in the case where an initial attempt is made
   * but also when the underlying channel is closed or the previous connect attempt was
   * unsuccessful.</p>
   */
  private void reconnect() {
    state.set(EndpointState.CONNECTING);

    final AtomicLong attemptStart = new AtomicLong();
    Mono
      .defer((Supplier<Mono<Channel>>) () -> {
        attemptStart.set(System.nanoTime());
        return channelFutureIntoMono(channelBootstrap.connect());
      })
      .timeout(endpointContext.environment().ioEnvironment().connectTimeout())
      .onErrorResume(throwable -> {
        if (disconnect.get()) {
          endpointContext.environment().eventBus().publish(
            new EndpointConnectionAbortedEvent(
              Duration.ofNanos(System.nanoTime() - attemptStart.get()),
              endpointContext,
              ConnectTimings.toMap(channel))
          );
          return Mono.empty();
        } else {
          return Mono.error(throwable);
        }
      })
      .retryWhen(Retry
        .any()
        .exponentialBackoff(Duration.ofMillis(32), Duration.ofMillis(4096))
        .retryMax(Long.MAX_VALUE)
        .doOnRetry(retryContext -> {
          Duration duration = retryContext.exception() instanceof TimeoutException
            ? endpointContext.environment().ioEnvironment().connectTimeout()
            : Duration.ofNanos(System.nanoTime() - attemptStart.get());
          endpointContext.environment().eventBus().publish(new EndpointConnectionFailedEvent(
            duration,
            endpointContext,
            retryContext.iteration(),
            retryContext.exception()
          ));
        })
      ).subscribe(
        channel -> {
          if (disconnect.get()) {
            this.channel = null;
            endpointContext.environment().eventBus().publish(new EndpointConnectionIgnoredEvent(
              Duration.ofNanos(System.nanoTime() - attemptStart.get()),
              endpointContext,
              ConnectTimings.toMap(channel)
            ));
            closeChannel(channel);
          } else {
            this.channel = channel;
            endpointContext.environment().eventBus().publish(new EndpointConnectedEvent(
              Duration.ofNanos(System.nanoTime() - attemptStart.get()),
              endpointContext,
              ConnectTimings.toMap(channel)
            ));
            this.circuitBreaker.reset();
            state.set(EndpointState.CONNECTED);
          }
        },
        error -> endpointContext.environment().eventBus().publish(
          new UnexpectedEndpointConnectionFailedEvent(Duration.ofNanos(
            System.nanoTime() - attemptStart.get()),
            endpointContext,
            error)
        )
      );
  }

  @Override
  public void disconnect() {
    if (disconnect.compareAndSet(false, true)) {
      state.set(EndpointState.DISCONNECTING);
      closeChannel(this.channel);
    }
  }

  /**
   * Helper method to close a channel and emit events if needed.
   *
   * <p>If no channel has been active already, it directly goes into a disconnected
   * state. If one has been there before, it waits until the disconnect future
   * completes.</p>
   *
   * @param channel the channel to close.
   */
  private void closeChannel(final Channel channel) {
    if (channel != null) {
      final long start = System.nanoTime();
      channel.disconnect().addListener(future -> {
        Duration latency = Duration.ofNanos(System.nanoTime() - start);
        state.set(EndpointState.DISCONNECTED);
        if (future.isSuccess()) {
          endpointContext.environment().eventBus().publish(
            new EndpointDisconnectedEvent(latency, endpointContext)
          );
        } else {
          endpointContext.environment().eventBus().publish(
            new EndpointDisconnectionFailedEvent(latency, endpointContext, future.cause())
          );
        }
      });
    } else {
      state.set(EndpointState.DISCONNECTED);
    }
  }

  @Override
  public <R extends Request<? extends Response>> void send(R request) {
    if (canWrite()) {
        circuitBreaker.track();
        outstandingRequests.incrementAndGet();
        request.response().whenComplete((r, t) -> {
          if (r != null) {
            circuitBreaker.markSuccess();
          } else {
            circuitBreaker.markFailure();
          }
          outstandingRequests.decrementAndGet();
          lastResponseTimestamp = System.nanoTime();
        });
        channel.writeAndFlush(request);
    } else {
      RetryOrchestrator.maybeRetry(endpointContext, request);
    }
  }

  @Override
  public boolean free() {
    return outstandingRequests.get() == 0;
  }

  @Override
  public long lastResponseReceived() {
    return lastResponseTimestamp;
  }

  /**
   * Helper method to check if we can write into the channel at this point.
   *
   * @return true if we can, false otherwise.
   */
  private boolean canWrite() {
    return state.get() == EndpointState.CONNECTED
      && channel.isActive()
      && channel.isWritable()
      && circuitBreaker.allowsRequest();
  }

  @Override
  public EndpointState state() {
    return state.get();
  }

  /**
   * Helper method to convert a netty {@link ChannelFuture} into an async {@link Mono}.
   *
   * <p>This method can be overridden in tests to fake certain responses from a
   * connect attempt.</p>
   *
   * @param channelFuture the future to convert/wrap.
   * @return the created mono.
   */
  protected Mono<Channel> channelFutureIntoMono(final ChannelFuture channelFuture) {
    CompletableFuture<Channel> completableFuture = new CompletableFuture<>();
    channelFuture.addListener((ChannelFutureListener) f -> {
      if (f.isSuccess()) {
        completableFuture.complete(f.channel());
      } else {
        completableFuture.completeExceptionally(f.cause());
      }
    });
    return Mono.fromFuture(completableFuture);
  }

}
