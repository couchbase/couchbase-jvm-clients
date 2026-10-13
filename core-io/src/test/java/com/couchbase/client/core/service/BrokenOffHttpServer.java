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

package com.couchbase.client.core.service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Locale;

import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * An HTTP server that answers each request with a response whose body breaks off: it says the body is longer
 * than what it sends, sends the start of it, and closes the connection. Like a connection that fails partway
 * through a response (for example, the node fails over while a query executes).
 * <p>
 * (The JDK's HTTP server keeps the connection open if a response is shorter than it said.)
 */
final class BrokenOffHttpServer implements AutoCloseable {
  private final ServerSocket serverSocket;
  private final Thread acceptor;

  /**
   * @param status the response status code
   * @param startOfBody what to send of the body before closing the connection
   */
  BrokenOffHttpServer(int status, String startOfBody) throws IOException {
    serverSocket = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
    byte[] body = startOfBody.getBytes(UTF_8);
    acceptor = new Thread(() -> {
      while (!serverSocket.isClosed()) {
        try (Socket socket = serverSocket.accept()) {
          readRequest(socket);
          OutputStream os = socket.getOutputStream();
          os.write(("HTTP/1.1 " + status + " Whatever\r\n"
            + "Content-Type: application/json\r\n"
            + "Content-Length: " + (body.length + 1024) + "\r\n"
            + "\r\n").getBytes(ISO_8859_1));
          os.write(body);
          os.flush();
        } catch (IOException e) {
          // closed
        }
      }
    }, "broken-off-http-server");
    acceptor.setDaemon(true);
    acceptor.start();
  }

  int port() {
    return serverSocket.getLocalPort();
  }

  /**
   * Reads the request headers and body (by its content length), so the client has sent the whole request
   * before the response arrives.
   */
  private static void readRequest(Socket socket) throws IOException {
    BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream(), ISO_8859_1));
    int contentLength = 0;
    String line;
    while ((line = in.readLine()) != null && !line.isEmpty()) {
      if (line.toLowerCase(Locale.ROOT).startsWith("content-length:")) {
        contentLength = Integer.parseInt(line.substring("content-length:".length()).trim());
      }
    }
    for (int i = 0; i < contentLength; i++) {
      if (in.read() == -1) {
        return;
      }
    }
  }

  @Override
  public void close() throws IOException {
    serverSocket.close();
  }
}
