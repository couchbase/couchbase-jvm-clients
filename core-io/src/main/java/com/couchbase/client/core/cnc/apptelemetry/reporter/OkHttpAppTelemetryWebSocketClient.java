/*
 * Copyright 2026 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.cnc.apptelemetry.reporter;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.cnc.apptelemetry.collector.AppTelemetryCollector;
import com.couchbase.client.core.deps.okhttp3.Response;
import com.couchbase.client.core.deps.okhttp3.WebSocket;
import com.couchbase.client.core.deps.okhttp3.WebSocketListener;
import com.couchbase.client.core.deps.okio.Buffer;
import com.couchbase.client.core.deps.okio.ByteString;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static com.couchbase.client.core.logging.RedactableArgument.redactSystem;
import static java.util.Objects.requireNonNull;

/**
 * Like {@link AppTelemetryWebSocketClient} (Netty), but using OkHttp.
 * <p>
 * OkHttp answers the server's pings, and pings the server itself.
 */
class OkHttpAppTelemetryWebSocketClient implements AppTelemetryClient {
  private static final Logger log = LoggerFactory.getLogger(OkHttpAppTelemetryWebSocketClient.class);

  /**
   * Ping the server this often. If the pong hasn't arrived by the next ping, the connection is dead.
   * <p>
   * Unlike the Netty implementation, OkHttp pings even if the connection isn't idle, and there's no way to ping
   * only when it's idle. The same interval as the Netty implementation, to keep the load on the server low,
   * at the cost of slower detection of a dead connection: up to two intervals after the last pong
   * (the Netty implementation detects it after 16 minutes).
   */
  private static final Duration pingInterval = Duration.ofMinutes(15);

  private static final int REQ_OPCODE_GET_TELEMETRY = 0;

  private static final int RES_OPCODE_SUCCESS = 0;
  private static final int RES_OPCODE_UNRECOGNIZED = 1;

  private static final int CLOSE_NORMAL = 1000;
  private static final int CLOSE_UNSUPPORTED_DATA = 1003;

  private final AppTelemetryCollector collector;
  private final CoreContext coreContext;

  OkHttpAppTelemetryWebSocketClient(
    CoreContext coreContext,
    AppTelemetryCollector collector
  ) {
    this.coreContext = requireNonNull(coreContext);
    this.collector = requireNonNull(collector);
  }

  @Override
  public void connectAndWaitForClose(URI remote, Runnable doOnSuccessfulConnection) throws InterruptedException {
    // Completes when the WebSocket closes, or exceptionally if it fails.
    CompletableFuture<@Nullable Void> closed = new CompletableFuture<>();

    WebSocket webSocket = coreContext.core().okHttpClient().newWebSocket(
      new com.couchbase.client.core.deps.okhttp3.Request.Builder().url(remote.toString()), // OkHttp takes the ws and wss schemes here
      !isUserSpecifiedEndpoint(), // useClusterTlsAndCredentials
      coreContext.environment().timeoutConfig().connectTimeout(),
      pingInterval,
      new Listener(remote, doOnSuccessfulConnection, closed)
    );

    try {
      closed.get();
    } catch (ExecutionException e) {
      throw new RuntimeException("App telemetry WebSocket failed", e.getCause());
    } finally {
      webSocket.cancel(); // does nothing if it's already closed
    }
  }

  /**
   * Returns true if the user specified an app telemetry endpoint (see
   * {@code com.couchbase.client.core.env.CoreEnvironment.Builder.appTelemetryEndpoint}),
   * instead of letting the cluster say where to send telemetry.
   * User-specified endpoints don't support authentication or TLS.
   */
  private boolean isUserSpecifiedEndpoint() {
    return coreContext.environment().appTelemetryEndpoint() != null;
  }

  private class Listener extends WebSocketListener {
    private final URI remote;
    private final Runnable doOnSuccessfulConnection;
    private final CompletableFuture<@Nullable Void> closed;
    private volatile boolean open;

    Listener(URI remote, Runnable doOnSuccessfulConnection, CompletableFuture<@Nullable Void> closed) {
      this.remote = requireNonNull(remote);
      this.doOnSuccessfulConnection = requireNonNull(doOnSuccessfulConnection);
      this.closed = requireNonNull(closed);
    }

    @Override
    public void onOpen(WebSocket webSocket, Response response) {
      log.info("App telemetry WebSocket connection established for remote: {}", redactSystem(remote));
      open = true;
      doOnSuccessfulConnection.run();
    }

    @Override
    public void onMessage(WebSocket webSocket, ByteString bytes) {
      int opcode = bytes.size() == 0 ? -1 : bytes.getByte(0) & 0xff;

      if (opcode == REQ_OPCODE_GET_TELEMETRY) {
        try (Buffer response = new Buffer()) {
          response.writeByte(RES_OPCODE_SUCCESS);
          collector.reportTo(s -> response.writeUtf8(s.toString()));

          if (log.isDebugEnabled()) {
            log.debug(
              "App telemetry WebSocket client responding to GET_TELEMETRY with:\n{}",
              response.snapshot()
                .substring(1) // skip response opcode
                .utf8()
                .trim() // without trailing newline
            );
          }

          webSocket.send(response.readByteString());
        }

      } else {
        log.debug("App telemetry WebSocket client responding to unrecognized opcode {}", opcode);
        webSocket.send(ByteString.of((byte) RES_OPCODE_UNRECOGNIZED));
      }
    }

    @Override
    public void onMessage(WebSocket webSocket, String text) {
      log.error("App telemetry WebSocket client received unexpected text message; closing.");
      webSocket.close(CLOSE_UNSUPPORTED_DATA, "Expected a binary message.");
    }

    @Override
    public void onClosing(WebSocket webSocket, int code, String reason) {
      log.debug("App telemetry WebSocket client received close frame; code={} reason={}", code, reason);
      webSocket.close(CLOSE_NORMAL, null); // finish the closing handshake
    }

    @Override
    public void onClosed(WebSocket webSocket, int code, String reason) {
      log.debug("App telemetry WebSocket closed.");
      closed.complete(null);
    }

    @Override
    public void onFailure(WebSocket webSocket, Throwable t, @Nullable Response response) {
      if (open && t instanceof EOFException) {
        // The server closed the connection without a close frame (for example, because the node restarted).
        // Like the Netty implementation, that's a close, not a failure.
        log.debug("App telemetry WebSocket connection closed by the server without a close frame.");
        closed.complete(null);
        return;
      }
      closed.completeExceptionally(t);
    }
  }
}
