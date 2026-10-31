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

package com.couchbase.client.core.io.kv;

import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.io.netty.kv.NettyMemcacheProtocol;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.jspecify.annotations.Nullable;

import javax.net.ServerSocketFactory;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noCas;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noDatatype;
import static com.couchbase.client.core.io.netty.kv.NettyMemcacheProtocol.noExtras;
import static com.couchbase.client.core.io.netty.kv.NettyMemcacheProtocol.noKey;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * A fake KV server for tests: speaks the memcached binary protocol on a loopback port, and answers the
 * connection handshake like Couchbase Server (HELLO, GET_ERROR_MAP, SASL with PLAIN, SELECT_BUCKET).
 * Other requests go to the handler the test sets.
 */
final class FakeKeyValueServer implements Closeable {
  static final String HOST = "127.0.0.1";
  static final String USERNAME = "username";
  static final String PASSWORD = "password";
  static final String BUCKET = "default";

  static final short STATUS_SUCCESS = 0x00;
  static final short STATUS_NOT_FOUND = 0x01;
  static final short STATUS_NOT_MY_VBUCKET = 0x07;
  static final short STATUS_NO_BUCKET = 0x08;
  static final short STATUS_AUTH_ERROR = 0x20;
  static final short STATUS_ACCESS_ERROR = 0x24;

  /**
   * Handles a request the handshake doesn't. Runs on the connection's thread.
   */
  @FunctionalInterface
  interface Handler {
    void handle(Connection connection, Packet request) throws IOException;
  }

  /**
   * A packet the client sent. Its bytes are copied, so it stays valid.
   */
  static final class Packet {
    final byte[] bytes;

    Packet(byte[] bytes) {
      this.bytes = bytes;
    }

    ByteBuf buf() {
      return Unpooled.wrappedBuffer(bytes);
    }

    byte opcode() {
      return NettyMemcacheProtocol.opcode(buf());
    }

    int opaque() {
      return NettyMemcacheProtocol.opaque(buf());
    }

    String key() {
      return NettyMemcacheProtocol.keyAsString(buf());
    }

    byte[] body() {
      return NettyMemcacheProtocol.bodyAsBytes(buf());
    }

    boolean is(MemcacheProtocol.Opcode opcode) {
      return opcode() == opcode.opcode();
    }

    @Override
    public String toString() {
      return MemcacheProtocol.Opcode.of(opcode()) + " (opaque " + opaque() + ")";
    }
  }

  /**
   * A connection from the client.
   */
  final class Connection implements Closeable {
    final Socket socket;
    private final DataInputStream in;
    private final OutputStream out;

    Connection(Socket socket) throws IOException {
      this.socket = socket;
      this.in = new DataInputStream(socket.getInputStream());
      this.out = socket.getOutputStream();
    }

    synchronized void send(byte[] packet) throws IOException {
      out.write(packet);
      out.flush();
    }

    void respond(Packet request, short status, byte[] body) throws IOException {
      respond(request.opcode(), request.opaque(), status, body);
    }

    void respond(Packet request, short status) throws IOException {
      respond(request, status, new byte[0]);
    }

    void respond(byte opcode, int opaque, short status, byte[] body) throws IOException {
      send(response(opcode, opaque, status, body));
    }

    /**
     * Returns a response packet's bytes (to send several at once).
     */
    byte[] response(Packet request, short status, byte[] body) {
      return response(request.opcode(), request.opaque(), status, body);
    }

    byte[] response(byte opcode, int opaque, short status, byte[] body) {
      ByteBuf response = NettyMemcacheProtocol.response(
        UnpooledByteBufAllocator.DEFAULT,
        MemcacheProtocol.Opcode.of(opcode),
        noDatatype(),
        status,
        opaque,
        noCas(),
        noExtras(),
        noKey(),
        Unpooled.wrappedBuffer(body)
      );
      byte[] bytes;
      try {
        bytes = new byte[response.readableBytes()];
        response.readBytes(bytes);
      } finally {
        response.release();
      }
      return bytes;
    }

    /**
     * Sends a CLUSTERMAP_CHANGE_NOTIFICATION, like the server does when the bucket's config changes.
     */
    void pushClustermapChangeNotification(String bucket, long epoch, long revision) throws IOException {
      byte[] key = bucket.getBytes(UTF_8);
      ByteBuf packet = Unpooled.buffer()
        .writeByte(MemcacheProtocol.Magic.SERVER_PUSH_REQUEST.magic())
        .writeByte(MemcacheProtocol.ServerPushOpcode.CLUSTERMAP_CHANGE_NOTIFICATION.opcode())
        .writeShort(key.length)
        .writeByte(16) // extras length
        .writeByte(0) // datatype
        .writeShort(0) // vbucket
        .writeInt(16 + key.length) // total body length
        .writeInt(0) // opaque
        .writeLong(0) // cas
        .writeLong(epoch)
        .writeLong(revision)
        .writeBytes(key);
      byte[] bytes;
      try {
        bytes = new byte[packet.readableBytes()];
        packet.readBytes(bytes);
      } finally {
        packet.release();
      }
      send(bytes);
    }

    /**
     * Reads the next packet, or returns null if the client closed the connection.
     */
    @Nullable Packet read() throws IOException {
      byte[] header = new byte[MemcacheProtocol.HEADER_SIZE];
      try {
        in.readFully(header);
      } catch (EOFException e) {
        return null;
      }
      int bodyLength = Unpooled.wrappedBuffer(header).getInt(8);
      byte[] packet = new byte[header.length + bodyLength];
      System.arraycopy(header, 0, packet, 0, header.length);
      in.readFully(packet, header.length, bodyLength);
      return new Packet(packet);
    }

    @Override
    public void close() {
      try {
        socket.close();
      } catch (IOException ignore) {
      }
    }
  }

  private final ServerSocket serverSocket;
  private final Thread acceptor;
  private final List<Connection> connections = new CopyOnWriteArrayList<>();
  private final BlockingQueue<Connection> accepted = new LinkedBlockingQueue<>();

  /**
   * Every packet the server read (handshake included), in order.
   */
  private final BlockingQueue<Packet> received = new LinkedBlockingQueue<>();

  // Settings tests can change before the client connects.
  volatile boolean answerHandshake = true;
  volatile boolean pipelinedSelectBucketFailsBeforeAuthentication = true;
  volatile String saslMechanisms = "PLAIN";
  volatile String expectedPassword = PASSWORD;
  volatile short selectBucketStatus = STATUS_SUCCESS;
  volatile @Nullable byte[] errorMap = null; // null means "fail GET_ERROR_MAP"

  private volatile Handler handler = (connection, request) -> connection.respond(request, STATUS_SUCCESS);

  static FakeKeyValueServer start() throws IOException {
    return start(ServerSocketFactory.getDefault());
  }

  static FakeKeyValueServer start(ServerSocketFactory serverSocketFactory) throws IOException {
    return start(serverSocketFactory.createServerSocket(0, 50, InetAddress.getByName(HOST)));
  }

  /**
   * Starts serving on the given server socket (for example, a TLS one configured by the test).
   */
  static FakeKeyValueServer start(ServerSocket serverSocket) {
    return new FakeKeyValueServer(serverSocket);
  }

  private FakeKeyValueServer(ServerSocket serverSocket) {
    this.serverSocket = serverSocket;
    this.acceptor = new Thread(this::acceptConnections, "fake-kv-acceptor");
    acceptor.setDaemon(true);
    acceptor.start();
  }

  int port() {
    return serverSocket.getLocalPort();
  }

  /**
   * Sets the handler for requests the handshake doesn't handle.
   */
  void handler(Handler handler) {
    this.handler = requireNonNull(handler);
  }

  /**
   * Returns the next connection the server accepted.
   */
  Connection nextConnection(Duration timeout) throws InterruptedException {
    Connection connection = accepted.poll(timeout.toMillis(), TimeUnit.MILLISECONDS);
    if (connection == null) {
      throw new AssertionError("No connection within " + timeout);
    }
    return connection;
  }

  /**
   * Returns the next packet the server read (with the given opcode, skipping others).
   */
  Packet nextReceived(MemcacheProtocol.Opcode opcode, Duration timeout) throws InterruptedException {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (true) {
      long remaining = deadline - System.nanoTime();
      Packet packet = received.poll(Math.max(0, remaining), TimeUnit.NANOSECONDS);
      if (packet == null) {
        throw new AssertionError("No " + opcode + " request within " + timeout);
      }
      if (packet.is(opcode)) {
        return packet;
      }
    }
  }

  List<Packet> receivedSoFar() {
    return new ArrayList<>(received);
  }

  private void acceptConnections() {
    while (!serverSocket.isClosed()) {
      try {
        Socket socket = serverSocket.accept();
        Connection connection = new Connection(socket);
        connections.add(connection);
        accepted.add(connection);
        Thread t = new Thread(() -> serve(connection), "fake-kv-connection");
        t.setDaemon(true);
        t.start();
      } catch (IOException e) {
        // closed
      }
    }
  }

  private void serve(Connection connection) {
    boolean authenticated = false;
    try {
      Packet request;
      while ((request = connection.read()) != null) {
        received.add(request);
        if (!answerHandshake && isHandshake(request)) {
          continue;
        }

        if (request.is(MemcacheProtocol.Opcode.HELLO)) {
          connection.respond(request, STATUS_SUCCESS, request.body()); // agrees to every feature
        } else if (request.is(MemcacheProtocol.Opcode.ERROR_MAP)) {
          byte[] map = errorMap;
          connection.respond(request, map == null ? STATUS_NOT_FOUND : STATUS_SUCCESS, map == null ? new byte[0] : map);
        } else if (request.is(MemcacheProtocol.Opcode.SASL_LIST_MECHS)) {
          connection.respond(request, STATUS_SUCCESS, saslMechanisms.getBytes(UTF_8));
        } else if (request.is(MemcacheProtocol.Opcode.SASL_AUTH)) {
          authenticated = authenticate(request);
          connection.respond(request, authenticated ? STATUS_SUCCESS : STATUS_AUTH_ERROR);
        } else if (request.is(MemcacheProtocol.Opcode.SELECT_BUCKET)) {
          short status = !authenticated && pipelinedSelectBucketFailsBeforeAuthentication
            ? STATUS_ACCESS_ERROR
            : selectBucketStatus;
          connection.respond(request, status);
        } else {
          handler.handle(connection, request);
        }
      }
    } catch (IOException e) {
      // The client closed the connection, or the TLS handshake failed (which the tests check on the client side).
    } finally {
      connection.close();
    }
  }

  private static boolean isHandshake(Packet request) {
    return request.is(MemcacheProtocol.Opcode.HELLO)
      || request.is(MemcacheProtocol.Opcode.ERROR_MAP)
      || request.is(MemcacheProtocol.Opcode.SASL_LIST_MECHS)
      || request.is(MemcacheProtocol.Opcode.SASL_AUTH)
      || request.is(MemcacheProtocol.Opcode.SELECT_BUCKET);
  }

  private boolean authenticate(Packet request) {
    if (!request.key().equals("PLAIN") || !saslMechanisms.contains("PLAIN")) {
      return false;
    }
    String expected = "\0" + USERNAME + "\0" + expectedPassword;
    return new String(request.body(), UTF_8).equals(expected);
  }

  /**
   * Closes every connection, but keeps accepting new ones.
   */
  void closeConnections() {
    connections.forEach(Connection::close);
  }

  @Override
  public void close() {
    try {
      serverSocket.close();
    } catch (IOException ignore) {
    }
    closeConnections();
  }
}
