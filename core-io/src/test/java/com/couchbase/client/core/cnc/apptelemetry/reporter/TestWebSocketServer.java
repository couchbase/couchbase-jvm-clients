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

import org.jspecify.annotations.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * A minimal WebSocket server (RFC 6455) for tests, on a plain {@link ServerSocket}.
 * <p>
 * Supports just what the app telemetry tests need: the opening handshake, sending binary messages
 * and close frames, receiving the client's binary messages, and answering pings. Each frame is
 * a whole message (no fragmentation), and there are no extensions (so no compression).
 * <p>
 * An independent implementation, so the tests check OkHttp's client against the protocol,
 * not against OkHttp's own server side.
 */
final class TestWebSocketServer implements Closeable {

  /**
   * The value the server combines with the client's key to prove it understood the handshake (RFC 6455, 1.3).
   */
  private static final String HANDSHAKE_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

  private static final int OPCODE_TEXT = 0x1;
  private static final int OPCODE_BINARY = 0x2;
  private static final int OPCODE_CLOSE = 0x8;
  private static final int OPCODE_PING = 0x9;
  private static final int OPCODE_PONG = 0xa;

  private final ServerSocket listener;
  private final String path;
  private final Consumer<Connection> onHandshakeComplete;
  private final List<Connection> connections = new CopyOnWriteArrayList<>();
  private final ExecutorService executor = Executors.newCachedThreadPool(r -> {
    Thread t = new Thread(r, "test-websocket-server");
    t.setDaemon(true);
    return t;
  });

  /**
   * Starts a server on a loopback address, which accepts connections until it's closed.
   *
   * @param path the only path the server accepts WebSocket connections on
   * @param onHandshakeComplete called (on the connection's thread) once a connection's handshake is complete,
   * before the server reads any messages from it
   */
  TestWebSocketServer(String path, Consumer<Connection> onHandshakeComplete) throws IOException {
    this.path = requireNonNull(path);
    this.onHandshakeComplete = requireNonNull(onHandshakeComplete);
    this.listener = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
    executor.execute(this::acceptConnections);
  }

  /**
   * The URI clients should connect to.
   */
  URI uri() {
    return URI.create("ws://127.0.0.1:" + listener.getLocalPort() + path);
  }

  @Override
  public void close() throws IOException {
    try {
      listener.close();
      for (Connection c : connections) {
        c.drop();
      }
    } finally {
      executor.shutdownNow();
    }
  }

  private void acceptConnections() {
    while (!listener.isClosed()) {
      try {
        Socket socket = listener.accept();
        Connection connection = new Connection(socket);
        connections.add(connection);
        executor.execute(connection::run);
      } catch (IOException e) {
        // The listener was closed.
        return;
      }
    }
  }

  /**
   * One client's connection.
   */
  final class Connection {
    private final Socket socket;
    private final OutputStream out;
    private volatile @Nullable String authorizationHeader;

    /**
     * The messages the client sent, in order.
     */
    final BlockingQueue<byte[]> receivedMessages = new LinkedBlockingQueue<>();

    /**
     * Completes when the connection is closed, by either side.
     */
    final CompletableFuture<Void> closed = new CompletableFuture<>();

    private Connection(Socket socket) throws IOException {
      this.socket = socket;
      this.out = socket.getOutputStream();
    }

    /**
     * The value of the handshake request's Authorization header, or null if it didn't have one.
     */
    @Nullable String authorizationHeader() {
      return authorizationHeader;
    }

    void sendBinary(byte[] message) {
      sendFrame(OPCODE_BINARY, message);
    }

    /**
     * Starts the closing handshake. Once the client answers with its own close frame, the server closes the connection.
     */
    void sendClose(int statusCode, String reason) {
      byte[] reasonBytes = reason.getBytes(UTF_8);
      byte[] payload = new byte[2 + reasonBytes.length];
      payload[0] = (byte) (statusCode >> 8);
      payload[1] = (byte) statusCode;
      System.arraycopy(reasonBytes, 0, payload, 2, reasonBytes.length);
      sendFrame(OPCODE_CLOSE, payload);
    }

    /**
     * Closes the connection without a close frame, as if the server went away.
     */
    void drop() {
      try {
        socket.close();
      } catch (IOException e) {
        // Closing anyway.
      }
      closed.complete(null);
    }

    private void run() {
      try {
        InputStream in = socket.getInputStream();
        if (!handshake(in, out)) {
          return;
        }
        onHandshakeComplete.accept(this);
        readMessages(new DataInputStream(in));
      } catch (IOException e) {
        // The connection was closed, by either side.
      } finally {
        drop();
      }
    }

    /**
     * Reads the client's upgrade request and answers it. Returns false (after answering with an error)
     * if it isn't a WebSocket upgrade request for the server's path.
     */
    private boolean handshake(InputStream in, OutputStream out) throws IOException {
      String[] lines = readRequestHead(in).split("\r\n");
      String[] requestLine = lines[0].split(" ");
      Map<String, String> headers = new HashMap<>();
      for (int i = 1; i < lines.length; i++) {
        int colon = lines[i].indexOf(':');
        if (colon > 0) {
          headers.put(lines[i].substring(0, colon).trim().toLowerCase(Locale.ROOT), lines[i].substring(colon + 1).trim());
        }
      }

      String key = headers.get("sec-websocket-key");
      if (requestLine.length < 2 || !requestLine[1].equals(path) || key == null
        || !"websocket".equalsIgnoreCase(headers.get("upgrade"))) {
        out.write("HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".getBytes(ISO_8859_1));
        out.flush();
        return false;
      }

      authorizationHeader = headers.get("authorization");
      out.write(("HTTP/1.1 101 Switching Protocols\r\n"
        + "Upgrade: websocket\r\n"
        + "Connection: Upgrade\r\n"
        + "Sec-WebSocket-Accept: " + acceptKey(key) + "\r\n"
        + "\r\n").getBytes(ISO_8859_1));
      out.flush();
      return true;
    }

    /**
     * Reads frames until the client closes the connection, or answers the server's close frame.
     */
    private void readMessages(DataInputStream in) throws IOException {
      while (true) {
        int first = in.readUnsignedByte();
        int second = in.readUnsignedByte();
        int opcode = first & 0x0f;
        boolean masked = (second & 0x80) != 0;

        long length = second & 0x7f;
        if (length == 126) {
          length = in.readUnsignedShort();
        } else if (length == 127) {
          length = in.readLong();
        }

        byte[] mask = new byte[4];
        if (masked) {
          in.readFully(mask);
        }
        byte[] payload = new byte[Math.toIntExact(length)];
        in.readFully(payload);
        if (masked) {
          for (int i = 0; i < payload.length; i++) {
            payload[i] ^= mask[i % 4];
          }
        }

        switch (opcode) {
          case OPCODE_BINARY:
          case OPCODE_TEXT:
            receivedMessages.add(payload);
            break;
          case OPCODE_PING:
            sendFrame(OPCODE_PONG, payload);
            break;
          case OPCODE_CLOSE:
            return; // the caller closes the connection
          default:
            break; // ignore pongs (and anything else)
        }
      }
    }

    /**
     * Writes an unmasked frame (servers don't mask) holding the whole payload.
     */
    private void sendFrame(int opcode, byte[] payload) {
      ByteArrayOutputStream frame = new ByteArrayOutputStream();
      frame.write(0x80 | opcode); // FIN, so this frame is the whole message
      if (payload.length < 126) {
        frame.write(payload.length);
      } else if (payload.length <= 0xffff) {
        frame.write(126);
        frame.write(payload.length >> 8);
        frame.write(payload.length);
      } else {
        frame.write(127);
        for (int shift = 56; shift >= 0; shift -= 8) {
          frame.write((int) ((long) payload.length >> shift));
        }
      }
      frame.write(payload, 0, payload.length);

      try {
        synchronized (out) {
          out.write(frame.toByteArray());
          out.flush();
        }
      } catch (IOException e) {
        throw new RuntimeException("Failed to send WebSocket frame", e);
      }
    }
  }

  /**
   * Reads the request line and headers, up to and including the blank line that ends them,
   * one byte at a time (so nothing after them is read).
   */
  private static String readRequestHead(InputStream in) throws IOException {
    ByteArrayOutputStream head = new ByteArrayOutputStream();
    int matched = 0; // how much of "\r\n\r\n" was just read
    while (matched < 4) {
      int b = in.read();
      if (b == -1) {
        throw new EOFException("Connection closed during the WebSocket handshake");
      }
      head.write(b);
      matched = (b == (matched % 2 == 0 ? '\r' : '\n')) ? matched + 1 : (b == '\r' ? 1 : 0);
    }
    return new String(head.toByteArray(), ISO_8859_1);
  }

  private static String acceptKey(String clientKey) {
    try {
      byte[] digest = MessageDigest.getInstance("SHA-1").digest((clientKey + HANDSHAKE_GUID).getBytes(ISO_8859_1));
      return Base64.getEncoder().encodeToString(digest);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
