/*
 * Copyright (c) 2026 Couchbase, Inc.
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

package io.netty.handler.codec.http2;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.logging.LogLevel;

/**
 * Replaces Netty's class of the same name in the shaded jar, so HTTP/2 frames are never logged.
 * <p>
 * gRPC's NettyClientHandler always logs HTTP/2 frames at DEBUG, using a logger named
 * after a class that is under "com.couchbase" once shaded. The frames include
 * request headers (like "Authorization") and the start of each message body.
 * A user who enabled debug logging for "com.couchbase" would see them.
 * <p>
 * The shade plugin excludes Netty's original, and relocates this class in its place.
 * The public API must match Netty's original; Http2FrameLoggerApiTest verifies it does.
 */
public class Http2FrameLogger extends ChannelHandlerAdapter {

  public enum Direction {
    INBOUND,
    OUTBOUND
  }

  public Http2FrameLogger(LogLevel level) {
  }

  public Http2FrameLogger(LogLevel level, String name) {
  }

  public Http2FrameLogger(LogLevel level, Class<?> clazz) {
  }

  public boolean isEnabled() {
    return false;
  }

  public void logData(Direction direction, ChannelHandlerContext ctx, int streamId, ByteBuf data, int padding, boolean endStream) {
  }

  public void logHeaders(Direction direction, ChannelHandlerContext ctx, int streamId, Http2Headers headers, int padding, boolean endStream) {
  }

  public void logHeaders(Direction direction, ChannelHandlerContext ctx, int streamId, Http2Headers headers, int streamDependency, short weight, boolean exclusive, int padding, boolean endStream) {
  }

  public void logPriority(Direction direction, ChannelHandlerContext ctx, int streamId, int streamDependency, short weight, boolean exclusive) {
  }

  public void logRstStream(Direction direction, ChannelHandlerContext ctx, int streamId, long errorCode) {
  }

  public void logSettingsAck(Direction direction, ChannelHandlerContext ctx) {
  }

  public void logSettings(Direction direction, ChannelHandlerContext ctx, Http2Settings settings) {
  }

  public void logPing(Direction direction, ChannelHandlerContext ctx, long data) {
  }

  public void logPingAck(Direction direction, ChannelHandlerContext ctx, long data) {
  }

  public void logPushPromise(Direction direction, ChannelHandlerContext ctx, int streamId, int promisedStreamId, Http2Headers headers, int padding) {
  }

  public void logGoAway(Direction direction, ChannelHandlerContext ctx, int lastStreamId, long errorCode, ByteBuf debugData) {
  }

  public void logWindowsUpdate(Direction direction, ChannelHandlerContext ctx, int streamId, int windowSizeIncrement) {
  }

  public void logUnknownFrame(Direction direction, ChannelHandlerContext ctx, byte frameType, int streamId, Http2Flags flags, ByteBuf data) {
  }
}
