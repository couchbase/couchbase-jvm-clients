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
import com.couchbase.client.core.endpoint.EndpointContext;
import com.couchbase.client.core.error.AuthenticationFailureException;
import com.couchbase.client.core.error.BucketNotFoundException;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.context.KeyValueIoErrorContext;
import com.couchbase.client.core.io.IoContext;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.deps.io.netty.channel.ChannelDuplexHandler;
import com.couchbase.client.core.deps.io.netty.channel.ChannelHandlerContext;
import com.couchbase.client.core.deps.io.netty.channel.ChannelPromise;
import com.couchbase.client.core.deps.io.netty.util.ReferenceCountUtil;
import com.couchbase.client.core.msg.kv.BaseKeyValueRequest;

import java.net.SocketAddress;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noBody;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noCas;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noDatatype;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noExtras;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noPartition;
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
    ctx.writeAndFlush(buildSelectBucketRequest(ctx));
  }

  @Override
  public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
    Optional<Duration> latency = ConnectTimings.stop(ctx.channel(), this.getClass(), false);

    if (msg instanceof ByteBuf) {
      short status = status((ByteBuf) msg);
      if (status == MemcacheProtocol.Status.SUCCESS.status()) {
        endpointContext.environment().eventBus().publish(new SelectBucketCompletedEvent(
          latency.orElse(Duration.ZERO),
          ioContext,
          bucketName)
        );
        interceptedConnectPromise.trySuccess();
        ctx.pipeline().remove(this);
        ctx.fireChannelActive();
      } else if (status == MemcacheProtocol.Status.ACCESS_ERROR.status()) {
        endpointContext.environment().eventBus().publish(
          new SelectBucketFailedEvent(Event.Severity.ERROR, ioContext, status)
        );
        interceptedConnectPromise.tryFailure(
          new AuthenticationFailureException(
            "Either the bucket with name \"" + redactMeta(bucketName) + "\" is not present or the user does not have " +
              "the right privileges to access it",
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
    } else{
      interceptedConnectPromise.tryFailure(
        new CouchbaseException("Unexpected response type on channel read, this is a bug " +
          "- please report." + msg)
      );
    }
    ReferenceCountUtil.release(msg);
  }

  /**
   * Helper method to build the select bucket request.
   *
   * @param ctx the {@link ChannelHandlerContext} for which the channel active operation is made.
   * @return the created request as a {@link ByteBuf}.
   */
  private ByteBuf buildSelectBucketRequest(final ChannelHandlerContext ctx) {
    ByteBuf key = Unpooled.copiedBuffer(bucketName, UTF_8);
    ByteBuf request = request(
      ctx.alloc(),
      MemcacheProtocol.Opcode.SELECT_BUCKET,
      noDatatype(),
      noPartition(),
      BaseKeyValueRequest.nextOpaque(),
      noCas(),
      noExtras(),
      key,
      noBody()
    );
    key.release();
    return request;
  }

}
