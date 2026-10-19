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
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
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

  @Test
  void hostNameLookupGivesUpWhenTheDeadlinePasses() throws Exception {
    CountDownLatch release = new CountDownLatch(1);
    try {
      KeyValueSocketConnector connector = newConnector(SocketFactory.getDefault(), host -> {
        // Like a DNS server that doesn't answer.
        try {
          release.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        throw new UnknownHostException(host);
      });

      long start = System.nanoTime();
      UnknownHostException e = assertThrows(UnknownHostException.class, () -> connector.connect(
        "node1.example",
        11210,
        System.nanoTime() + Duration.ofMillis(200).toNanos(),
        socket -> {},
        () -> {}
      ));

      assertTrue(e.getMessage().matches("node1\\.example: timed out after \\d+ ms"), e.getMessage());
      Duration elapsed = Duration.ofNanos(System.nanoTime() - start);
      assertTrue(elapsed.compareTo(Duration.ofMillis(150)) >= 0 && elapsed.compareTo(Duration.ofSeconds(5)) < 0, elapsed.toString());
    } finally {
      release.countDown();
    }
  }

  @Test
  void connectAttemptsShareALookupInProgress() throws Exception {
    AtomicInteger lookups = new AtomicInteger();
    CountDownLatch release = new CountDownLatch(1);
    try {
      KeyValueSocketConnector connector = newConnector(new TimingOutSocketFactory(), host -> {
        lookups.incrementAndGet();
        try {
          release.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        return InetAddress.getLoopbackAddress();
      });

      // Both give up waiting for the same lookup.
      for (int i = 0; i < 2; i++) {
        assertThrows(UnknownHostException.class, () -> connector.connect(
          "node1.example",
          11210,
          System.nanoTime() + Duration.ofMillis(100).toNanos(),
          socket -> {},
          () -> {}
        ));
      }
      assertEquals(1, lookups.get());

      // Once it's done, the next connect attempt looks up the host again. (The socket factory's connect times out.)
      release.countDown();
      long giveUp = System.nanoTime() + Duration.ofSeconds(5).toNanos();
      while (lookups.get() < 2 && System.nanoTime() < giveUp) {
        assertThrows(ConnectException.class, () -> connector.connect(
          "node1.example",
          11210,
          System.nanoTime() + Duration.ofSeconds(5).toNanos(),
          socket -> {},
          () -> {}
        ));
      }
      assertEquals(2, lookups.get());
    } finally {
      release.countDown();
    }
  }

  @Test
  void failedHostNameLookupIsReported() {
    UnknownHostException lookupFailure = new UnknownHostException("node1.example: nodename nor servname provided, or not known");
    KeyValueSocketConnector connector = newConnector(SocketFactory.getDefault(), host -> {
      throw lookupFailure;
    });

    UnknownHostException e = assertThrows(UnknownHostException.class, () -> connector.connect(
      "node1.example",
      11210,
      System.nanoTime() + Duration.ofSeconds(10).toNanos(),
      socket -> {},
      () -> {}
    ));
    assertEquals(lookupFailure.getMessage(), e.getMessage());
    assertSame(lookupFailure, e.getCause());
  }

  private static KeyValueSocketConnector newConnector(SocketFactory socketFactory, KeyValueSocketConnector.HostResolver hostResolver) {
    return new KeyValueSocketConnector(
      socketFactory,
      SecurityConfig.builder().build(),
      PasswordAuthenticator.create("user", "password"),
      Duration.ofSeconds(10),
      hostResolver
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
