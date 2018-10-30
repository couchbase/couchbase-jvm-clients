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
import com.couchbase.client.core.io.NetworkAddress;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;
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

import java.nio.channels.Pipe;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

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
  private final CoreContext coreContext;

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
   * Once connected, contains the channel to work with.
   */
  private volatile Channel channel;

  BaseEndpoint(final NetworkAddress hostname, final int port, final EventLoopGroup eventLoopGroup,
               final CoreContext coreContext) {
    this.coreContext = coreContext;
    this.state = new AtomicReference<>(EndpointState.DISCONNECTED);
    disconnect = new AtomicBoolean(false);

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

    channelFutureIntoMono(channelBootstrap.connect())
      .timeout(coreContext.environment().ioEnvironment().connectTimeout())
      .flatMap((Function<Channel, Mono<Channel>>) channel -> disconnect.get() ? Mono.empty() : Mono.just(channel))
      .onErrorResume(throwable -> disconnect.get() ? Mono.empty() : Mono.error(throwable))
      .retryBackoff(Long.MAX_VALUE, Duration.ofMillis(32), Duration.ofMillis(4096))
      .subscribe(channel -> {
        if (disconnect.get()) {
          state.set(EndpointState.DISCONNECTED);
          // todo: we succeeded to connect but got instructed to disconnect in the
          // todo: meantime
        } else {
          this.channel = channel;
          state.set(EndpointState.CONNECTED_CIRCUIT_CLOSED);
          // todo: debug log we are connected.
        }
      });
  }

  @Override
  public void disconnect() {
    if (disconnect.compareAndSet(false, true)) {
      state.set(EndpointState.DISCONNECTING);
      if (channel != null) {
        channel.disconnect().addListener(new ChannelFutureListener() {
          @Override
          public void operationComplete(ChannelFuture future) throws Exception {
            state.set(EndpointState.DISCONNECTED);
            // todo: track and log proper shutdown
          }
        });
      }
    }
  }

  @Override
  public <R extends Request<? extends Response>> void send(R request) {
    if (state.get() == EndpointState.CONNECTED_CIRCUIT_CLOSED) {
      // todo: make sure channel is writable and active before writing?
      channel.writeAndFlush(request);
    } else {
      // todo: handle me all cases
    }
  }

  @Override
  public EndpointState state() {
    return state.get();
  }

  /**
   * Helper method to convert a netty {@link ChannelFuture} into an async {@link Mono}.
   *
   * @param channelFuture the future to convert/wrap.
   * @return the created mono.
   */
  private static Mono<Channel> channelFutureIntoMono(final ChannelFuture channelFuture) {
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
