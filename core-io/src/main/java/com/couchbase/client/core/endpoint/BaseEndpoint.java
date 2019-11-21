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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.events.endpoint.EndpointConnectionAbortedEvent;
import com.couchbase.client.core.cnc.events.endpoint.EndpointConnectionFailedEvent;
import com.couchbase.client.core.cnc.events.endpoint.EndpointConnectedEvent;
import com.couchbase.client.core.cnc.events.endpoint.EndpointConnectionIgnoredEvent;
import com.couchbase.client.core.cnc.events.endpoint.EndpointDisconnectedEvent;
import com.couchbase.client.core.cnc.events.endpoint.EndpointDisconnectionFailedEvent;
import com.couchbase.client.core.cnc.events.endpoint.EndpointStateChangedEvent;
import com.couchbase.client.core.cnc.events.endpoint.UnexpectedEndpointConnectionFailedEvent;
import com.couchbase.client.core.cnc.events.endpoint.UnexpectedEndpointDisconnectedEvent;
import com.couchbase.client.core.deps.io.netty.channel.DefaultEventLoopGroup;
import com.couchbase.client.core.deps.io.netty.channel.epoll.EpollChannelOption;
import com.couchbase.client.core.deps.io.netty.channel.local.LocalChannel;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.SecurityConfig;
import com.couchbase.client.core.io.netty.PipelineErrorHandler;
import com.couchbase.client.core.io.netty.SslHandlerFactory;
import com.couchbase.client.core.io.netty.TrafficCaptureHandler;
import com.couchbase.client.core.io.netty.kv.ChannelAttributes;
import com.couchbase.client.core.io.netty.kv.ConnectTimings;
import com.couchbase.client.core.msg.CancellationReason;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.retry.RetryOrchestrator;
import com.couchbase.client.core.retry.RetryReason;
import com.couchbase.client.core.retry.reactor.Retry;
import com.couchbase.client.core.service.ServiceContext;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.deps.io.netty.bootstrap.Bootstrap;
import com.couchbase.client.core.deps.io.netty.channel.Channel;
import com.couchbase.client.core.deps.io.netty.channel.ChannelFuture;
import com.couchbase.client.core.deps.io.netty.channel.ChannelFutureListener;
import com.couchbase.client.core.deps.io.netty.channel.ChannelInitializer;
import com.couchbase.client.core.deps.io.netty.channel.ChannelOption;
import com.couchbase.client.core.deps.io.netty.channel.ChannelPipeline;
import com.couchbase.client.core.deps.io.netty.channel.EventLoopGroup;
import com.couchbase.client.core.deps.io.netty.channel.epoll.EpollEventLoopGroup;
import com.couchbase.client.core.deps.io.netty.channel.epoll.EpollSocketChannel;
import com.couchbase.client.core.deps.io.netty.channel.kqueue.KQueueEventLoopGroup;
import com.couchbase.client.core.deps.io.netty.channel.kqueue.KQueueSocketChannel;
import com.couchbase.client.core.deps.io.netty.channel.nio.NioEventLoopGroup;
import com.couchbase.client.core.deps.io.netty.channel.socket.nio.NioSocketChannel;
import com.couchbase.client.core.diag.EndpointHealth;
import com.couchbase.client.core.util.SingleStateful;
import com.couchbase.client.core.util.HostAndPort;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;

/**
 * This {@link BaseEndpoint} implements all common logic for endpoints that wrap the IO layer.
 *
 * <p>In addition to just wrapping a netty channel, this implementation is also a circuit breaker
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
  private final SingleStateful<EndpointState> state;

  /**
   * The related context to use.
   */
  private final AtomicReference<EndpointContext> endpointContext;

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
   * Check if the circuit breaker is enabled.
   */
  private final boolean circuitBreakerEnabled;

  /**
   * If the current endpoint is free or not.
   *
   * <p>Note that the value is not tracked if pipelined = true, since we only need it
   * to check if the endpoint is free or not. And it is always free for pipelined endpoints.</p>
   */
  private final AtomicInteger outstandingRequests;

  /**
   * The event loop group used for this endpoint, passed to netty.
   */
  private final EventLoopGroup eventLoopGroup;

  private final RequestCompletionConsumer requestCompletionConsumer = new RequestCompletionConsumer();

  private final ServiceType serviceType;

  /**
   * If this endpoint supports pipelining.
   */
  private final boolean pipelined;

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
   * @param serviceContext the core context.
   * @param circuitBreakerConfig the circuit breaker config used.
   */
  BaseEndpoint(final String hostname, final int port, final EventLoopGroup eventLoopGroup,
               final ServiceContext serviceContext, final CircuitBreakerConfig circuitBreakerConfig,
               final ServiceType serviceType, final boolean pipelined) {
    disconnect = new AtomicBoolean(false);
    this.pipelined = pipelined;
    if (circuitBreakerConfig.enabled()) {
      this.circuitBreaker = new LazyCircuitBreaker(circuitBreakerConfig);
      this.circuitBreakerEnabled = true;
    } else {
      this.circuitBreaker = NoopCircuitBreaker.INSTANCE;
      this.circuitBreakerEnabled = false;
    }
    this.endpointContext = new AtomicReference<>(
      new EndpointContext(serviceContext, new HostAndPort(hostname, port), circuitBreaker, serviceType,
        Optional.empty(), serviceContext.bucket(), Optional.empty())
    );
    this.state = SingleStateful.fromInitial(
      EndpointState.DISCONNECTED,
      (from, to) -> serviceContext.environment().eventBus().publish(new EndpointStateChangedEvent(endpointContext.get(), from, to))
    );

    this.outstandingRequests = new AtomicInteger(0);
    this.lastResponseTimestamp = 0;
    this.eventLoopGroup = eventLoopGroup;
    this.serviceType = serviceType;
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
    } else if (eventLoopGroup instanceof DefaultEventLoopGroup) {
      // Used for testing!
      return LocalChannel.class;
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
    if (state.compareAndTransition(EndpointState.DISCONNECTED, EndpointState.CONNECTING)) {
      reconnect();
    }
  }

  /**
   * Helper method to create the remote address this endpoint will (re)connect to.
   *
   * <p>Note that this method has been refactored out so it can be overridden for local testing.</p>
   */
  protected SocketAddress remoteAddress() {
    final EndpointContext ctx = endpointContext.get();
    return InetSocketAddress.createUnresolved(ctx.remoteSocket().hostname(), ctx.remoteSocket().port());
  }

  /**
   * This method performs the actual connecting logic.
   *
   * <p>It is called reconnect since it works both in the case where an initial attempt is made
   * but also when the underlying channel is closed or the previous connect attempt was
   * unsuccessful.</p>
   */
  private void reconnect() {
    if (disconnect.get()) {
      return;
    }
    state.transition(EndpointState.CONNECTING);

    final EndpointContext endpointContext = this.endpointContext.get();

    final AtomicLong attemptStart = new AtomicLong();
    Mono
      .defer((Supplier<Mono<Channel>>) () -> {
        CoreEnvironment env = endpointContext.environment();

        long connectTimeoutMs = env
          .timeoutConfig()
          .connectTimeout()
          .toMillis();

        if (eventLoopGroup.isShutdown()) {
          throw new IllegalStateException("Event Loop is already shut down, not pursuing connect attempt!");
        }

        final Bootstrap channelBootstrap = new Bootstrap()
          .remoteAddress(remoteAddress())
          .group(eventLoopGroup)
          .channel(channelFrom(eventLoopGroup))
          .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) connectTimeoutMs)
          .handler(new ChannelInitializer<Channel>() {
            @Override
            protected void initChannel(final Channel ch) {
              ChannelPipeline pipeline = ch.pipeline();

              SecurityConfig config = env.securityConfig();
              if (config.tlsEnabled()) {
                try {
                  pipeline.addFirst(SslHandlerFactory.get(ch.alloc(), config, endpointContext.authenticator()));
                } catch (Exception e) {
                  throw new SecurityException("Could not instantiate SSL Handler", e);
                }
              }
              if (env.ioConfig().captureTraffic().contains(serviceType)) {
                pipeline.addLast(new TrafficCaptureHandler(endpointContext));
              }
              pipelineInitializer().init(BaseEndpoint.this, pipeline);
              pipeline.addLast(new PipelineErrorHandler(BaseEndpoint.this));
            }
          });

        if (env.ioConfig().tcpKeepAlivesEnabled() && !(eventLoopGroup instanceof DefaultEventLoopGroup)) {
          channelBootstrap.option(ChannelOption.SO_KEEPALIVE, true);
          if (eventLoopGroup instanceof EpollEventLoopGroup) {
            channelBootstrap.option(
              EpollChannelOption.TCP_KEEPIDLE,
              (int) TimeUnit.MILLISECONDS.toSeconds(env.ioConfig().tcpKeepAliveTime().toMillis()));
          }
        }

        attemptStart.set(System.nanoTime());
        return channelFutureIntoMono(channelBootstrap.connect());
      })
      .timeout(endpointContext.environment().timeoutConfig().connectTimeout())
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
            ? endpointContext.environment().timeoutConfig().connectTimeout()
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

            Optional<HostAndPort> localSocket = Optional.empty();
            if (channel.localAddress() instanceof InetSocketAddress) {
              // it will always be an inet socket address, but to safeguard for testing mocks...
              InetSocketAddress so = (InetSocketAddress) channel.localAddress();
              localSocket = Optional.of(new HostAndPort(so.getHostString(), so.getPort()));
            }

            EndpointContext newContext = new EndpointContext(
              endpointContext,
              endpointContext.remoteSocket(),
              endpointContext.circuitBreaker(),
              endpointContext.serviceType(),
              localSocket,
              endpointContext.bucket(),
              Optional.ofNullable(channel.attr(ChannelAttributes.CHANNEL_ID_KEY).get())
            );
            this.endpointContext.get().environment().eventBus().publish(new EndpointConnectedEvent(
              Duration.ofNanos(System.nanoTime() - attemptStart.get()),
              newContext,
              ConnectTimings.toMap(channel)
            ));
            this.endpointContext.set(newContext);
            this.circuitBreaker.reset();
            state.transition(EndpointState.CONNECTED);
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
      state.transition(EndpointState.DISCONNECTING);
      closeChannel(this.channel);
    }
  }

  /**
   * This method is called from inside the channel to tell the endpoint hat it got inactive.
   *
   * <p>The endpoint needs to perform certain steps when the channel is inactive so that it quickly tries
   * to reconnect, as long as it should (i.e. don't do it if already disconnected)</p>
   */
  @Stability.Internal
  public void notifyChannelInactive() {
    outstandingRequests.set(0);
    if (disconnect.get()) {
      // We don't need to do anything if we've been already instructed to disconnect.
      return;
    }

    if (state() == EndpointState.CONNECTED) {
      endpointContext.get().environment().eventBus().publish(
        new UnexpectedEndpointDisconnectedEvent(endpointContext.get())
      );

      state.transition(EndpointState.DISCONNECTED);
      connect();
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
    if (channel != null && !channel.eventLoop().isShutdown()) {
      final EndpointContext endpointContext = this.endpointContext.get();
      final long start = System.nanoTime();

      channel.disconnect().addListener(future -> {
        Duration latency = Duration.ofNanos(System.nanoTime() - start);
        state.transition(EndpointState.DISCONNECTED);
        state.close();
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
      state.transition(EndpointState.DISCONNECTED);
      state.close();
    }
  }

  @Override
  public <R extends Request<? extends Response>> void send(final R request) {
    if (request.timeoutElapsed()) {
      request.cancel(CancellationReason.TIMEOUT);
    }
    if (request.completed()) {
      return;
    }

    final EndpointContext ctx = endpointContext.get();
    if (canWrite()) {
      request.context()
        .lastDispatchedFrom(ctx.localSocket().orElse(null))
        .lastDispatchedTo(ctx.remoteSocket())
        .lastChannelId(ctx.channelId().orElse(null));

      if (!pipelined) {
        outstandingRequests.incrementAndGet();
      }
      if (circuitBreakerEnabled) {
        circuitBreaker.track();
        request.response().whenComplete(requestCompletionConsumer);
      }
      channel.writeAndFlush(request);
    } else {
      RetryOrchestrator.maybeRetry(ctx, request, RetryReason.ENDPOINT_NOT_WRITABLE);
    }
  }

  @Override
  public boolean free() {
    return pipelined || outstandingRequests.get() == 0;
  }

  @Override
  public long lastResponseReceived() {
    return lastResponseTimestamp;
  }

  /**
   * Called from the event loop handlers to mark a request as being completed.
   *
   * <p>We need to make this call explicitly from the outside and cannot just listen on the request response
   * callback because with streaming responses the actual completion might happen much later.</p>
   */
  @Stability.Internal
  public void markRequestCompletion() {
    decrementOutstandingRequests();
    lastResponseTimestamp = System.nanoTime();
  }

  /**
   * Helper method to decrement outstanding requests, even if they haven't finished yet.
   */
  @Stability.Internal
  public void decrementOutstandingRequests() {
    if (!pipelined) {
      outstandingRequests.decrementAndGet();
    }
  }

  /**
   * Helper method to check if we can write into the channel at this point.
   *
   * @return true if we can, false otherwise.
   */
  private boolean canWrite() {
    return state.state() == EndpointState.CONNECTED
      && channel.isActive()
      && channel.isWritable()
      && circuitBreaker.allowsRequest()
      && free();
  }

  @Override
  public EndpointState state() {
    return state.state();
  }

  @Override
  public Flux<EndpointState> states() {
    return state.states();
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

  /**
   * Returns the current endpoint context for external use.
   */
  public EndpointContext endpointContext() {
    return endpointContext.get();
  }

  /**
   * Returns true if this endpoint is pipelined, false otherwise.
   */
  public boolean pipelined() {
    return pipelined;
  }

  /**
   * This request completion consumer is cached in the parent class to reuse it
   * across each request and not create garbage each and every time.
   *
   * <p>It gets called when a request is completed and updates the endpoints associated
   * state i.e. circuit breakers, outstanding requests and last response timestamp.</p>
   */
  class RequestCompletionConsumer implements BiConsumer<Response, Throwable> {
    @Override
    public void accept(final Response r, final Throwable t) {
      if (r != null) {
        circuitBreaker.markSuccess();
      } else {
        circuitBreaker.markFailure();
      }
    }
  }

  @Override
  public EndpointHealth diagnostics() {
    String remote = null;
    String local = null;
    if(channel != null) {
      SocketAddress remoteAddr = channel.remoteAddress();
      SocketAddress localAddr = channel.localAddress();
      if (remoteAddr instanceof InetSocketAddress) {
        InetSocketAddress ra = (InetSocketAddress) remoteAddr;
        remote = redactMeta(ra.getHostString()) + ":" + ra.getPort();
      }
      if (localAddr instanceof InetSocketAddress) {
        InetSocketAddress la = (InetSocketAddress) localAddr;
        local = redactMeta(la.getHostString()) + ":" + la.getPort();
      }
    }
    long lastActivity = TimeUnit.NANOSECONDS.toMicros(lastResponseTimestamp > 0 ? System.nanoTime() - lastResponseTimestamp : 0);
    String id = "0x" + Integer.toHexString(hashCode());

    return new EndpointHealth(endpointContext().serviceType(),
            state(),
            remote,
            local,
            endpointContext().bucket(),
            lastActivity,
            id);
  }
}
