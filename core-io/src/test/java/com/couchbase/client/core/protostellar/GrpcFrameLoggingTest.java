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

package com.couchbase.client.core.protostellar;

import com.couchbase.client.core.deps.io.grpc.ManagedChannel;
import com.couchbase.client.core.deps.io.grpc.Metadata;
import com.couchbase.client.core.deps.io.grpc.Server;
import com.couchbase.client.core.deps.io.grpc.StatusRuntimeException;
import com.couchbase.client.core.deps.io.grpc.netty.NettyChannelBuilder;
import com.couchbase.client.core.deps.io.grpc.netty.NettyServerBuilder;
import com.couchbase.client.core.deps.io.grpc.stub.MetadataUtils;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufUtil;
import com.couchbase.client.protostellar.kv.v1.GetRequest;
import com.couchbase.client.protostellar.kv.v1.KvServiceGrpc;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.WriterAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import static com.couchbase.client.core.deps.io.grpc.Metadata.ASCII_STRING_MARSHALLER;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the shaded gRPC Netty client does not log HTTP/2 frames
 * (which include credentials and document contents) when a user
 * enables debug logging for "com.couchbase".
 */
class GrpcFrameLoggingTest {
  private static final String CAPTURE_LOGGER = "com.couchbase";
  private static final String SECRET_HEADER_VALUE = "Basic c2VjcmV0LWNyZWRlbnRpYWxz";
  private static final String SECRET_DOCUMENT_ID = "secret-document-id";
  private static final String CAPTURE_CHECK = "Verifying debug messages are captured";

  private final StringWriter captured = new StringWriter();
  private WriterAppender appender;

  @BeforeEach
  void captureDebugLogs() {
    LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
    Configuration config = ctx.getConfiguration();

    appender = WriterAppender.newBuilder()
      .setName("GrpcFrameLoggingTest")
      .setTarget(captured)
      .setLayout(PatternLayout.newBuilder().withPattern("%c %m%n").build())
      .build();
    appender.start();

    LoggerConfig loggerConfig = new LoggerConfig(CAPTURE_LOGGER, Level.TRACE, false);
    loggerConfig.addAppender(appender, null, null);
    config.addLogger(CAPTURE_LOGGER, loggerConfig);
    ctx.updateLoggers();
  }

  @AfterEach
  void restoreLogConfig() {
    LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
    ctx.getConfiguration().removeLogger(CAPTURE_LOGGER);
    ctx.updateLoggers();
    appender.stop();
  }

  @Test
  void http2FramesAreNotLogged() throws Exception {
    Server server = NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", 0))
      .addService(new KvServiceGrpc.KvServiceImplBase() {
      })
      .build()
      .start();

    ManagedChannel channel = NettyChannelBuilder.forAddress("127.0.0.1", server.getPort())
      .usePlaintext()
      .build();

    LoggerFactory.getLogger(GrpcFrameLoggingTest.class).debug(CAPTURE_CHECK);

    try {
      Metadata headers = new Metadata();
      headers.put(Metadata.Key.of("Authorization", ASCII_STRING_MARSHALLER), SECRET_HEADER_VALUE);

      KvServiceGrpc.KvServiceBlockingStub stub = KvServiceGrpc.newBlockingStub(channel)
        .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers))
        .withDeadlineAfter(10, TimeUnit.SECONDS);

      // The server doesn't implement any methods, so this fails with UNIMPLEMENTED
      // after the request headers and body have been sent and received.
      assertThrows(StatusRuntimeException.class, () -> stub.get(GetRequest.newBuilder()
        .setBucketName("default")
        .setKey(SECRET_DOCUMENT_ID)
        .build()));

    } finally {
      channel.shutdownNow().awaitTermination(10, TimeUnit.SECONDS);
      server.shutdownNow().awaitTermination(10, TimeUnit.SECONDS);
    }

    String logs = captured.toString();
    assertTrue(logs.contains(CAPTURE_CHECK), "Log capture isn't working:\n" + logs);
    assertFalse(logs.contains(SECRET_HEADER_VALUE), "Authorization header value was logged:\n" + logs);
    assertFalse(logs.contains(SECRET_DOCUMENT_ID), "Request body was logged:\n" + logs);
    assertFalse(logs.contains(ByteBufUtil.hexDump(SECRET_DOCUMENT_ID.getBytes(UTF_8))), "Request body was logged (hex):\n" + logs);
  }
}
