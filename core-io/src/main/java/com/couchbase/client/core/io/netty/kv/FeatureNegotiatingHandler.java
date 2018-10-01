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

package com.couchbase.client.core.io.netty.kv;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.cnc.events.io.FeaturesNegotiatedEvent;
import com.couchbase.client.core.cnc.events.io.FeaturesNegotiationFailureEvent;
import com.couchbase.client.core.cnc.events.io.UnsolicitedFeaturesReturnedEvent;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.io.IoContext;
import com.couchbase.client.core.io.netty.ConnectTimings;
import com.couchbase.client.core.json.Mapper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.ReferenceCountUtil;

import java.net.SocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.couchbase.client.core.io.netty.kv.Protocol.status;

/**
 * The {@link FeatureNegotiatingHandler} is responsible for sending the KV "hello" command
 * and to handshake enabled features on both sides.
 *
 * <p>If we get any response from the server, we'll take it. If the server returns a
 * non-successful response we will report that, but move on with no negotiated features. If
 * the server returns more features than we asked for, we'll only use the subset and not
 * more (and report the abnormal condition).</p>
 *
 * @since 2.0.0
 */
class FeatureNegotiatingHandler extends ChannelDuplexHandler {

  private final Duration timeout;
  private final Set<ServerFeature> features;
  private final CoreContext coreContext;
  private IoContext ioContext;

  /**
   * Creates a new {@link FeatureNegotiatingHandler}.
   *
   * @param coreContext the core context used to refer to values like the core id.
   * @param timeout     how long the feature negotiation is allowed to take before the connect
   *                    process will be failed.
   * @param features    the list of features that should be negotiated from the client side.
   */
  FeatureNegotiatingHandler(final CoreContext coreContext, final Duration timeout,
                            final Set<ServerFeature> features) {
    this.coreContext = coreContext;
    this.timeout = timeout;
    this.features = features;
  }

  /**
   * Holds the intercepted promise from up the pipeline which is either
   * completed or failed depending on the downstream components or the
   * result of the hello negotiation.
   */
  private ChannelPromise interceptedConnectPromise;

  /**
   * Intercepts the connect process inside the pipeline to only propagate either
   * success or failure if the hello process is completed either way.
   *
   * <p>Note that if no feature is to negotiate we can bail out right away.</p>
   *
   * @param ctx           the {@link ChannelHandlerContext} for which the connect operation is made.
   * @param remoteAddress the {@link SocketAddress} to which it should connect.
   * @param localAddress  the {@link SocketAddress} which is used as source on connect.
   * @param promise       the {@link ChannelPromise} to notify once the operation completes.
   */
  @Override
  public void connect(final ChannelHandlerContext ctx, final SocketAddress remoteAddress,
                      final SocketAddress localAddress, final ChannelPromise promise) {
    ioContext = new IoContext(coreContext, localAddress, remoteAddress);

    if (features.isEmpty()) {
      ConnectTimings.record(ctx.channel(), this.getClass());
      ctx.pipeline().remove(this);
      ctx.connect(remoteAddress, localAddress, promise);
    } else {
      interceptedConnectPromise = promise;
      ChannelPromise downstream = ctx.newPromise();
      downstream.addListener(f -> {
        if (!f.isSuccess() && !interceptedConnectPromise.isDone()) {
          ConnectTimings.record(ctx.channel(), this.getClass());
          interceptedConnectPromise.tryFailure(f.cause());
        }
      });
      ctx.connect(remoteAddress, localAddress, downstream);
    }
  }

  /**
   * As soon as the channel is active start sending the hello request but also schedule
   * a timeout properly.
   *
   * @param ctx the {@link ChannelHandlerContext} for which the channel active operation is made.
   */
  @Override
  public void channelActive(final ChannelHandlerContext ctx) {
    ctx.executor().schedule(() -> {
      if (!interceptedConnectPromise.isDone()) {
        ConnectTimings.stop(ctx.channel(), this.getClass(), true);
        interceptedConnectPromise.tryFailure(
          new TimeoutException("KV Feature Negotiation timed out after "
            + timeout.toMillis() + "ms")
        );
      }
    }, timeout.toNanos(), TimeUnit.NANOSECONDS);
    ConnectTimings.start(ctx.channel(), this.getClass());
    ctx.writeAndFlush(buildHelloRequest(ctx));
  }

  @Override
  public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
    Optional<Duration> latency = ConnectTimings.stop(ctx.channel(), this.getClass(), false);

    if (msg instanceof ByteBuf) {
      if (status((ByteBuf) msg) != Protocol.STATUS_SUCCESS) {
        coreContext.env().eventBus().publish(
          new FeaturesNegotiationFailureEvent(ioContext, status((ByteBuf) msg))
        );
      }
      List<ServerFeature> negotiated = extractFeaturesFromBody((ByteBuf) msg);
      ctx.channel().attr(ServerFeature.SERVER_FEATURE_KEY).set(negotiated);
      coreContext.env().eventBus().publish(
        new FeaturesNegotiatedEvent(ioContext, latency.orElse(Duration.ZERO), negotiated)
      );
      interceptedConnectPromise.trySuccess();
      ctx.pipeline().remove(this);
      ctx.fireChannelActive();
    } else {
      interceptedConnectPromise.tryFailure(new CouchbaseException("Unexpected response "
        + "type on channel read, this is a bug - please report. " + msg));
    }

    ReferenceCountUtil.release(msg);
  }

  /**
   * Helper method to safely extract the negotiated server features from the
   * body of the memcache payload.
   *
   * @param response the response to extract from.
   * @return the list of server features, may be empty but never null.
   */
  private List<ServerFeature> extractFeaturesFromBody(final ByteBuf response) {
    Optional<ByteBuf> body = Protocol.body(response);
    List<ServerFeature> negotiated = new ArrayList<>();
    List<ServerFeature> unsolicited = new ArrayList<>();

    if (!body.isPresent()) {
      return negotiated;
    }

    while (body.get().isReadable()) {
      try {
        ServerFeature feature = ServerFeature.from(body.get().readShort());
        if (features.contains(feature)) {
          negotiated.add(feature);
        } else {
          unsolicited.add(feature);
        }
      } catch (Exception ex) {
        interceptedConnectPromise.tryFailure(new CouchbaseException(
          "Error while parsing negotiated server features.",
          ex
        ));
      }
    }

    if (!unsolicited.isEmpty()) {
      coreContext.env().eventBus().publish(
        new UnsolicitedFeaturesReturnedEvent(ioContext, unsolicited)
      );
    }
    return negotiated;
  }

  /**
   * Helper method to build the HELLO request which will be sent to the server.
   *
   * @param ctx the {@link ChannelHandlerContext} for which the channel active operation is made.
   * @return the created request as a {@link ByteBuf}.
   */
  private ByteBuf buildHelloRequest(final ChannelHandlerContext ctx) {
    ByteBuf key = buildHelloKey(ctx);

    ByteBuf body = ctx.alloc().buffer(features.size() * 2);
    for (ServerFeature feature : features) {
      body.writeShort(feature.value());
    }

    return Protocol.request(ctx.alloc(), Protocol.OPCODE_HELLO, key, body);
  }

  /**
   * Helper method which builds the "key" of the HELLO command. The key is made up
   * of the user agent as well as a pair of IDs that uniquely identify a socket.
   *
   * <p>In the unlikely event of the agent not being present, a dummy value is sent which
   * should at least help to distinguish the SDK language.</p>
   *
   * @param ctx the {@link ChannelHandlerContext} for which the channel active operation is made.
   * @return a {@link ByteBuf} with the full request to send.
   */
  private ByteBuf buildHelloKey(final ChannelHandlerContext ctx) {
    TreeMap<String, String> result = new TreeMap<>();

    String agent = coreContext.env().userAgent();
    if (agent == null || agent.isEmpty()) {
      agent = "java-core-io/unknown";
    } else if (agent.length() > 200) {
      agent = agent.substring(0, 200);
    }
    result.put("a", agent);

    String channelId = ctx.channel().id().asShortText();
    String paddedChannelId = paddedHex(channelId.equals("embedded") ? 1L : Long.decode(channelId));
    result.put("i", paddedHex(coreContext.id()) + "/" + paddedChannelId);

    return ctx.alloc().buffer().writeBytes(Mapper.encodeAsBytes(result));
  }

  /**
   * Pad the long input into a string encoded hex value.
   *
   * @param input the number to format.
   * @return the padded long hex value.
   */
  private static String paddedHex(long input) {
    return String.format("%016X", input);
  }

}