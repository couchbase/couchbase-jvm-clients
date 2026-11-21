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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.Event;
import com.couchbase.client.core.cnc.events.io.SelectBucketCompletedEvent;
import com.couchbase.client.core.cnc.events.io.SelectBucketFailedEvent;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.endpoint.EndpointContext;
import com.couchbase.client.core.error.AuthenticationFailureException;
import com.couchbase.client.core.error.BucketNotFoundException;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.context.KeyValueIoErrorContext;
import com.couchbase.client.core.io.IoContext;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.deps.io.netty.channel.ChannelDuplexHandler;
import com.couchbase.client.core.deps.io.netty.channel.ChannelHandlerContext;
import com.couchbase.client.core.deps.io.netty.channel.ChannelPromise;
import com.couchbase.client.core.deps.io.netty.util.ReferenceCountUtil;
import com.couchbase.client.core.msg.kv.BaseKeyValueRequest;
import org.jspecify.annotations.Nullable;

import java.net.SocketAddress;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.bodyAsBytes;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noBody;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noCas;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noDatatype;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noExtras;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noKey;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noPartition;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.opaque;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.request;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.status;
import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * The {@link SelectBucketHandler} is responsible for, selecting the right
 * bucket on this KV connection.
 *
 * <p>The reason this handler is in place that since Server 5.0 and RBAC
 * (role based access control) one user after SASL/cert auth can have
 * credentials for more than one bucket. To signal the server which bucket
 * it should select, this command is used as usually the last step in
 * the pipeline.</p>
 *
 * <p>It also fetches the selected bucket's config in the same round trip, and stores the bucket's UUID in
 * {@link ChannelAttributes#BUCKET_UUID_KEY}. The UUID tells the connection which incarnation of the bucket it is
 * talking to, so it can use collection IDs from that incarnation only. A bucket that is dropped and recreated
 * with the same name gets a new UUID, and its collection IDs start again from the beginning.</p>
 *
 * @since 2.0.0
 */
@Stability.Internal
public class SelectBucketHandler extends ChannelDuplexHandler {

  /**
   * Holds the core context as reference to event bus and more.
   */
  private final EndpointContext endpointContext;

  /**
   * Holds the timeout for the full select bucket loading phase.
   */
  private final Duration timeout;

  /**
   * The bucket name to select.
   */
  private final String bucketName;

  /**
   * Once connected, holds the io context for more debug information.
   */
  private IoContext ioContext;

  /**
   * Holds the intercepted promise from up the pipeline which is either
   * completed or failed depending on the downstream components or the
   * result of the select bucket negotiation.
   */
  private ChannelPromise interceptedConnectPromise;

  private int selectBucketOpaque;
  private int getConfigOpaque;
  private boolean selectBucketSucceeded;
  private boolean getConfigCompleted;

  /**
   * Creates a new {@link SelectBucketHandler}.
   *
   * @param endpointContext the core context used to refer to values like the core id.
   * @param bucketName  the bucket name to select.
   */
  public SelectBucketHandler(final EndpointContext endpointContext, final String bucketName) {
    this.endpointContext = endpointContext;
    this.timeout = endpointContext.environment().timeoutConfig().connectTimeout();
    this.bucketName = bucketName;
  }

  @Override
  public void connect(final ChannelHandlerContext ctx, final SocketAddress remoteAddress,
                      final SocketAddress localAddress, final ChannelPromise promise) {
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

  @Override
  public void channelActive(final ChannelHandlerContext ctx) {
    ioContext = new IoContext(
      endpointContext,
      ctx.channel().localAddress(),
      ctx.channel().remoteAddress(),
      endpointContext.bucket()
    );

    ctx.executor().schedule(() -> {
      if (!interceptedConnectPromise.isDone()) {
        ConnectTimings.stop(ctx.channel(), this.getClass(), true);
        interceptedConnectPromise.tryFailure(
          new TimeoutException("KV Select Bucket loading timed out after "
            + timeout.toMillis() + "ms")
        );
      }
    }, timeout.toNanos(), TimeUnit.NANOSECONDS);
    ConnectTimings.start(ctx.channel(), this.getClass());

    // The config request is pipelined behind SELECT_BUCKET, so it's answered for the selected bucket.
    selectBucketOpaque = BaseKeyValueRequest.nextOpaque();
    getConfigOpaque = BaseKeyValueRequest.nextOpaque();
    ctx.write(buildSelectBucketRequest(ctx, selectBucketOpaque));
    ctx.writeAndFlush(buildGetConfigRequest(ctx, getConfigOpaque));
  }

  @Override
  public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
    try {
      if (!(msg instanceof ByteBuf)) {
        interceptedConnectPromise.tryFailure(
          new CouchbaseException("Unexpected response type on channel read, this is a bug " +
            "- please report." + msg)
        );
        return;
      }

      ByteBuf response = (ByteBuf) msg;
      int opaque = opaque(response);
      if (opaque == selectBucketOpaque) {
        handleSelectBucketResponse(ctx, response);
      } else if (opaque == getConfigOpaque) {
        handleGetConfigResponse(ctx, response);
      } else {
        interceptedConnectPromise.tryFailure(
          new CouchbaseException("Unexpected response with opaque " + opaque + " during select bucket")
        );
        return;
      }
      maybeComplete(ctx);
    } finally {
      ReferenceCountUtil.release(msg);
    }
  }

  private void handleSelectBucketResponse(ChannelHandlerContext ctx, ByteBuf response) {
    Optional<Duration> latency = ConnectTimings.stop(ctx.channel(), this.getClass(), false);

    short status = status(response);
    if (status == MemcacheProtocol.Status.SUCCESS.status()) {
      endpointContext.environment().eventBus().publish(new SelectBucketCompletedEvent(
        latency.orElse(Duration.ZERO),
        ioContext,
        bucketName)
      );
      selectBucketSucceeded = true;
    } else if (status == MemcacheProtocol.Status.ACCESS_ERROR.status()) {
      // If the promise has been already completed with an error from a downstream handler, there is no
      // need to WARN because it will just spam and reduce the visibility of the actual source error.
      Event.Severity severity = interceptedConnectPromise.isDone() && !interceptedConnectPromise.isSuccess()
        ? Event.Severity.DEBUG
        : Event.Severity.WARN;

      endpointContext.environment().eventBus().publish(
        new SelectBucketFailedEvent(severity, ioContext, status)
      );
      // We don't set AuthenticationStatus.FAILED here, as it's not unambiguously that.
      // We'll be using the GCCCP endpoints anyway for determining RetryReason.AUTHENTICATION_FAILED.
      interceptedConnectPromise.tryFailure(
        new AuthenticationFailureException(
          "Either the bucket with name \"" + redactMeta(bucketName) + "\" is not present, the user does not have " +
            "the right privileges to access it, or the bucket is hibernated",
          new KeyValueIoErrorContext(MemcacheProtocol.decodeStatus(status), endpointContext, null),
          null
        )
      );
    } else if (status == MemcacheProtocol.Status.NOT_FOUND.status()) {
      // NOT_FOUND severity is lowered to debug because it shows up when bootstrapping against
      // a node in a MDS setup without the kv service / bucket enabled. If the bucket is legit not
      // present, we'd get the access error from above (which is an error).
      endpointContext.environment().eventBus().publish(
        new SelectBucketFailedEvent(Event.Severity.DEBUG, ioContext, status)
      );
      interceptedConnectPromise.tryFailure(BucketNotFoundException.forBucket(bucketName));
    } else {
      endpointContext.environment().eventBus().publish(
        new SelectBucketFailedEvent(Event.Severity.ERROR, ioContext, status)
      );
      interceptedConnectPromise.tryFailure(
        new CouchbaseException("Select bucket failed with unexpected status code 0x"
          + Integer.toHexString(status))
      );
    }
  }

  private void handleGetConfigResponse(ChannelHandlerContext ctx, ByteBuf response) {
    getConfigCompleted = true;
    if (status(response) != MemcacheProtocol.Status.SUCCESS.status()) {
      // The connection carries on without knowing the bucket's UUID; see BUCKET_UUID_KEY for when and why.
      return;
    }
    String bucketUuid = parseBucketUuid(response);
    if (bucketUuid != null) {
      ctx.channel().attr(ChannelAttributes.BUCKET_UUID_KEY).set(bucketUuid);
    }
  }

  private void maybeComplete(ChannelHandlerContext ctx) {
    if (selectBucketSucceeded && getConfigCompleted && interceptedConnectPromise.trySuccess()) {
      ctx.pipeline().remove(this);
      ctx.fireChannelActive();
    }
  }

  private static @Nullable String parseBucketUuid(ByteBuf response) {
    try {
      byte[] body = bodyAsBytes(response);
      if (body.length == 0) {
        return null;
      }
      JsonNode uuid = Mapper.decodeIntoTree(body).get("uuid");
      return uuid != null && uuid.isTextual() ? uuid.asText() : null;
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Helper method to build the select bucket request.
   *
   * @param ctx the {@link ChannelHandlerContext} for which the channel active operation is made.
   * @param opaque the request's opaque.
   * @return the created request as a {@link ByteBuf}.
   */
  private ByteBuf buildSelectBucketRequest(final ChannelHandlerContext ctx, int opaque) {
    ByteBuf key = Unpooled.copiedBuffer(bucketName, UTF_8);
    ByteBuf request = request(
      ctx.alloc(),
      MemcacheProtocol.Opcode.SELECT_BUCKET,
      noDatatype(),
      noPartition(),
      opaque,
      noCas(),
      noExtras(),
      key,
      noBody()
    );
    key.release();
    return request;
  }

  /**
   * Builds a request for the selected bucket's config. It deliberately doesn't send a known version,
   * because a recreated bucket could have the same version as the old one, and the UUID is needed either way.
   */
  private static ByteBuf buildGetConfigRequest(ChannelHandlerContext ctx, int opaque) {
    return request(
      ctx.alloc(),
      MemcacheProtocol.Opcode.GET_CONFIG,
      noDatatype(),
      noPartition(),
      opaque,
      noCas(),
      noExtras(),
      noKey(),
      noBody()
    );
  }

}
