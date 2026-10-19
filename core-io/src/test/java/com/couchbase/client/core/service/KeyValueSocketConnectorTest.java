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

import com.couchbase.client.core.endpoint.BaseEndpoint;
import com.couchbase.client.core.env.PasswordAuthenticator;
import com.couchbase.client.core.env.SecurityConfig;
import org.junit.jupiter.api.Test;

import javax.net.SocketFactory;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KeyValueSocketConnectorTest {

  @Test
  void connectTimeoutIsAConnectExceptionLikeNettys() {
    KeyValueSocketConnector connector = new KeyValueSocketConnector(
      new TimingOutSocketFactory(),
      SecurityConfig.builder().build(),
      PasswordAuthenticator.create("user", "password"),
      Duration.ofSeconds(10)
    );

    ConnectException e = assertThrows(ConnectException.class, () -> connector.connect(
      "127.0.0.1",
      11210,
      System.nanoTime() + Duration.ofSeconds(10).toNanos(),
      socket -> {},
      () -> {}
    ));

    assertTrue(e.getMessage().matches("connection timed out after \\d+ ms: /127\\.0\\.0\\.1:11210"), e.getMessage());
    assertInstanceOf(SocketTimeoutException.class, e.getCause());
    assertEquals(
      e.getMessage() + " - Check server ports and cluster encryption setting.",
      BaseEndpoint.annotateConnectException(e).getMessage()
    );
  }

  private static class TimingOutSocketFactory extends SocketFactory {
    @Override
    public Socket createSocket() {
      return new Socket() {
        @Override
        public void connect(SocketAddress endpoint, int timeout) throws SocketTimeoutException {
          throw new SocketTimeoutException("Connect timed out");
        }
      };
    }

    @Override
    public Socket createSocket(String host, int port) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Socket createSocket(String host, int port, InetAddress localHost, int localPort) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Socket createSocket(InetAddress host, int port) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Socket createSocket(InetAddress address, int port, InetAddress localAddress, int localPort) {
      throw new UnsupportedOperationException();
    }
  }
}
