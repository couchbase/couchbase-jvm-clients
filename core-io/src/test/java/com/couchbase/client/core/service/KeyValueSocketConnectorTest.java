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
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class KeyValueSocketConnectorTest {

  @Test
  void connectTimeoutIsAConnectExceptionLikeNettys() {
    KeyValueSocketConnector connector = newConnector(SocketFactory.getDefault(), InetAddress::getByName);

    // 192.0.2.1 is in TEST-NET-1 (RFC 5737). Where the network drops packets to it (rather than rejecting them),
    // connecting times out.
    IOException e = assertThrows(IOException.class, () -> connector.connectChannel(
      "192.0.2.1",
      11210,
      System.nanoTime() + Duration.ofMillis(300).toNanos(),
      channel -> {},
      waiter -> {},
      () -> {}
    ));
    assumeTrue(e instanceof ConnectException && e.getMessage().startsWith("connection timed out"),
      "this network rejects 192.0.2.1 instead of dropping it: " + e);

    assertTrue(e.getMessage().matches("connection timed out after \\d+ ms: /192\\.0\\.2\\.1:11210"), e.getMessage());
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
      UnknownHostException e = assertThrows(UnknownHostException.class, () -> connector.connectChannel(
        "node1.example",
        11210,
        System.nanoTime() + Duration.ofMillis(200).toNanos(),
        channel -> {},
        waiter -> {},
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
      int closedPort;
      try (ServerSocket socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
        closedPort = socket.getLocalPort();
      }
      KeyValueSocketConnector connector = newConnector(SocketFactory.getDefault(), host -> {
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
        assertThrows(UnknownHostException.class, () -> connector.connectChannel(
          "node1.example",
          11210,
          System.nanoTime() + Duration.ofMillis(100).toNanos(),
          channel -> {},
          waiter -> {},
          () -> {}
        ));
      }
      assertEquals(1, lookups.get());

      // Once it's done, the next connect attempt looks up the host again. (Connecting to the closed port is refused.)
      release.countDown();
      long giveUp = System.nanoTime() + Duration.ofSeconds(5).toNanos();
      while (lookups.get() < 2 && System.nanoTime() < giveUp) {
        assertThrows(ConnectException.class, () -> connector.connectChannel(
          "node1.example",
          closedPort,
          System.nanoTime() + Duration.ofSeconds(5).toNanos(),
          channel -> {},
          waiter -> {},
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

    UnknownHostException e = assertThrows(UnknownHostException.class, () -> connector.connectChannel(
      "node1.example",
      11210,
      System.nanoTime() + Duration.ofSeconds(10).toNanos(),
      channel -> {},
      waiter -> {},
      () -> {}
    ));
    assertEquals(lookupFailure.getMessage(), e.getMessage());
    assertSame(lookupFailure, e.getCause());
  }

  @Test
  void connectChannelClosesTheWaiterWhenItFailsWithAnError() throws Exception {
    KeyValueSocketConnector connector = newConnector(SocketFactory.getDefault(), host -> InetAddress.getLoopbackAddress());
    AtomicReference<KeyValueChannelWaiter> waiter = new AtomicReference<>();
    // For example, an AssertionError from the TLS channel.
    AssertionError failure = new AssertionError("failed after connecting");

    // The server socket's backlog accepts the connection, so connecting succeeds.
    try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
      AssertionError e = assertThrows(AssertionError.class, () -> connector.connectChannel(
        "node1.example",
        server.getLocalPort(),
        System.nanoTime() + Duration.ofSeconds(10).toNanos(),
        channel -> {},
        waiter::set,
        () -> {
          throw failure;
        }
      ));
      assertSame(failure, e);
    }
    assertTrue(waiter.get().isClosed());
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
}
