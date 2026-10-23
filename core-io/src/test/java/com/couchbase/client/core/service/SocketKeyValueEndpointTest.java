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

import com.couchbase.client.core.Core;
import com.couchbase.client.core.CoreResources;
import com.couchbase.client.core.cnc.Event;
import com.couchbase.client.core.cnc.EventBus.PublishResult;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.RequestTracer;
import com.couchbase.client.core.cnc.SimpleEventBus;
import com.couchbase.client.core.cnc.events.endpoint.EndpointConnectedEvent;
import com.couchbase.client.core.cnc.events.endpoint.EndpointConnectionFailedEvent;
import com.couchbase.client.core.cnc.events.endpoint.UnexpectedEndpointDisconnectedEvent;
import com.couchbase.client.core.cnc.events.io.ChannelClosedProactivelyEvent;
import com.couchbase.client.core.cnc.events.io.CustomTlsCiphersEnabledEvent;
import com.couchbase.client.core.cnc.events.io.FeaturesNegotiatedEvent;
import com.couchbase.client.core.cnc.events.io.GenericFailureDetectedEvent;
import com.couchbase.client.core.cnc.events.io.InvalidPacketDetectedEvent;
import com.couchbase.client.core.cnc.events.io.ReadTrafficCapturedEvent;
import com.couchbase.client.core.cnc.events.io.SaslAuthenticationCompletedEvent;
import com.couchbase.client.core.cnc.events.io.SecureConnectionFailedEvent;
import com.couchbase.client.core.cnc.events.io.UnknownResponseReceivedEvent;
import com.couchbase.client.core.cnc.events.io.WriteTrafficCapturedEvent;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufAllocator;
import com.couchbase.client.core.cnc.tracing.NoopRequestSpan;
import com.couchbase.client.core.config.ConfigurationProvider;
import com.couchbase.client.core.config.ProposedBucketConfigContext;
import com.couchbase.client.core.diagnostics.AuthenticationStatus;
import com.couchbase.client.core.diagnostics.EndpointDiagnostics;
import com.couchbase.client.core.endpoint.EndpointState;
import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.JwtAuthenticator;
import com.couchbase.client.core.env.PasswordAuthenticator;
import com.couchbase.client.core.env.SaslMechanism;
import com.couchbase.client.core.error.AuthenticationFailureException;
import com.couchbase.client.core.error.BucketNotFoundException;
import com.couchbase.client.core.error.EncodingFailureException;
import com.couchbase.client.core.error.RequestCanceledException;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.io.CollectionMap;
import com.couchbase.client.core.io.KeyValueRequestEncoder;
import com.couchbase.client.core.io.netty.kv.KeyValueChannelContext;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol.Opcode;
import com.couchbase.client.core.io.netty.kv.ServerFeature;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.kv.GetRequest;
import com.couchbase.client.core.msg.kv.GetResponse;
import com.couchbase.client.core.msg.kv.KeyValueRequest;
import com.couchbase.client.core.msg.kv.NoopRequest;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import com.couchbase.client.core.retry.FailFastRetryStrategy;
import com.couchbase.client.core.retry.RetryReason;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.topology.TopologyRevision;
import com.couchbase.client.core.util.CbThreads;
import com.couchbase.client.core.util.StorageSize;
import com.couchbase.client.core.util.SynchronousEventBus;
import okhttp3.tls.HandshakeCertificates;
import okhttp3.tls.HeldCertificate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.publisher.Mono;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.couchbase.client.core.service.FakeKeyValueServer.BUCKET;
import static com.couchbase.client.core.service.FakeKeyValueServer.HOST;
import static com.couchbase.client.core.service.FakeKeyValueServer.PASSWORD;
import static com.couchbase.client.core.service.FakeKeyValueServer.STATUS_NOT_FOUND;
import static com.couchbase.client.core.service.FakeKeyValueServer.STATUS_NOT_MY_VBUCKET;
import static com.couchbase.client.core.service.FakeKeyValueServer.STATUS_NO_BUCKET;
import static com.couchbase.client.core.service.FakeKeyValueServer.STATUS_SUCCESS;
import static com.couchbase.client.core.service.FakeKeyValueServer.USERNAME;
import static com.couchbase.client.core.service.OkHttpTestSupport.newTestOkHttpClient;
import static com.couchbase.client.core.service.OkHttpTlsHelper.CERTIFICATE_HINT;
import static com.couchbase.client.core.service.OkHttpTlsHelper.CLIENT_CERTIFICATE_HINT;
import static com.couchbase.client.core.service.OkHttpTlsHelper.CLIENT_CERTIFICATE_REQUIRED_HINT;
import static com.couchbase.client.core.util.MockUtil.mockCore;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Connects {@link SocketKeyValueEndpoint} to a fake KV server, and checks what it sends,
 * and how it handles what the server does.
 */
class SocketKeyValueEndpointTest {
  private static final Duration TIMEOUT = Duration.ofSeconds(10);
  private static final CollectionIdentifier DEFAULT_COLLECTION = CollectionIdentifier.fromDefault(BUCKET);

  private final List<AutoCloseable> cleanup = new ArrayList<>();

  private SimpleEventBus eventBus;

  /**
   * Called with each event, on the publishing thread.
   */
  private volatile Consumer<Event> onPublish = event -> {
  };

  private CoreEnvironment env;
  private CouchbaseOkHttpClient okHttpClient;
  private Core core;
  private ConfigurationProvider configurationProvider;
  private SynchronousEventBus<Core.AuthenticationRefreshTrigger> authenticationRefreshTriggers;
  private FakeKeyValueServer server;

  private void start(Consumer<CoreEnvironment.Builder<?>> envCustomizer) throws Exception {
    eventBus = new SimpleEventBus(true) {
      @Override
      public PublishResult publish(Event event) {
        PublishResult result = super.publish(event);
        onPublish.accept(event);
        return result;
      }
    };
    CoreEnvironment.Builder<?> builder = CoreEnvironment.builder().eventBus(eventBus);
    envCustomizer.accept(builder);
    env = builder.build();
    cleanup.add(env::shutdown);

    okHttpClient = newTestOkHttpClient(env);
    cleanup.add(okHttpClient);

    core = mockCore(env);
    configurationProvider = mock(ConfigurationProvider.class);
    when(configurationProvider.collectionMap()).thenReturn(new CollectionMap());
    when(core.configurationProvider()).thenReturn(configurationProvider);

    // A mock Core's fields are null.
    authenticationRefreshTriggers = new SynchronousEventBus<>();
    Field field = Core.class.getField("authenticationRefreshTriggers");
    field.setAccessible(true);
    field.set(core, authenticationRefreshTriggers);
  }

  private void start() throws Exception {
    start(builder -> {
    });
  }

  private void startServer() throws Exception {
    server = FakeKeyValueServer.start();
    cleanup.add(0, server);
  }

  @AfterEach
  void tearDown() throws Exception {
    for (AutoCloseable it : cleanup) {
      it.close();
    }
  }

  private static Authenticator plainAuthenticator(String password) {
    return PasswordAuthenticator.builder(USERNAME, password)
      .allowedSaslMechanisms(EnumSet.of(SaslMechanism.PLAIN))
      .build();
  }

  private SocketKeyValueEndpoint newEndpoint() {
    return newEndpoint(plainAuthenticator(PASSWORD), server.port());
  }

  private SocketKeyValueEndpoint newEndpoint(Authenticator authenticator, int port) {
    ServiceContext serviceContext = new ServiceContext(core.context(), HOST, port, ServiceType.KV, Optional.of(BUCKET));
    KeyValueSocketConnector connector = KeyValueSocketConnector.create(
      okHttpClient, env.securityConfig(), authenticator, env.timeoutConfig().connectTimeout()
    );
    SocketKeyValueEndpoint endpoint = new SocketKeyValueEndpoint(serviceContext, HOST, port, Optional.of(BUCKET), authenticator, connector);
    cleanup.add(0, endpoint::disconnect);
    return endpoint;
  }

  private SocketKeyValueEndpoint connectedEndpoint() throws Exception {
    SocketKeyValueEndpoint endpoint = newEndpoint();
    endpoint.connect();
    waitUntil(() -> endpoint.state() == EndpointState.CONNECTED);
    return endpoint;
  }

  private GetRequest newGetRequest(String key, RetryStrategy retryStrategy) {
    return new GetRequest(key, TIMEOUT, core.context(), DEFAULT_COLLECTION, retryStrategy, null);
  }

  private GetRequest newGetRequest(String key) {
    return newGetRequest(key, BestEffortRetryStrategy.INSTANCE);
  }

  private static void waitUntil(BooleanSupplier condition) throws InterruptedException {
    long deadline = System.nanoTime() + TIMEOUT.toNanos();
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() > deadline) {
        throw new AssertionError("Condition not met within " + TIMEOUT);
      }
      Thread.sleep(10);
    }
  }

  private <T extends Event> List<T> events(Class<T> type) {
    return eventBus.publishedEvents().stream()
      .filter(type::isInstance)
      .map(type::cast)
      .collect(Collectors.toList());
  }

  private <T extends Event> T waitForEvent(Class<T> type) throws InterruptedException {
    waitUntil(() -> !events(type).isEmpty());
    return events(type).get(0);
  }

  @Test
  void connectsAndNegotiates() throws Exception {
    start();
    startServer();
    SocketKeyValueEndpoint endpoint = connectedEndpoint();

    // In the order the Netty implementation sends them: SELECT_BUCKET is pipelined with SASL_AUTH, since PLAIN has one round trip.
    List<Opcode> handshake = server.receivedSoFar().stream()
      .map(it -> Opcode.of(it.opcode()))
      .collect(Collectors.toList());
    assertEquals(
      listOf(
        Opcode.HELLO,
        Opcode.ERROR_MAP,
        Opcode.SASL_LIST_MECHS,
        Opcode.SASL_AUTH,
        Opcode.SELECT_BUCKET
      ),
      handshake
    );

    FakeKeyValueServer.Packet hello = server.nextReceived(Opcode.HELLO, TIMEOUT);
    String helloKey = hello.key();
    assertTrue(helloKey.contains("\"i\":\"" + endpoint.context().channelId().orElseThrow(AssertionError::new) + "\""), helloKey);

    FeaturesNegotiatedEvent negotiated = waitForEvent(FeaturesNegotiatedEvent.class);
    assertTrue(negotiated.negotiated().contains(ServerFeature.COLLECTIONS));

    // (The endpoint's context is replaced once it connects, so its authentication status is unknown, like BaseEndpoint's.)
    waitForEvent(SaslAuthenticationCompletedEvent.class);
    EndpointConnectedEvent connected = waitForEvent(EndpointConnectedEvent.class);
    assertTrue(connected.timings().containsKey("SelectBucketHandler"), connected.timings().toString());

    EndpointDiagnostics diagnostics = endpoint.diagnostics();
    assertEquals(EndpointState.CONNECTED, diagnostics.state());
    assertEquals(Optional.of(BUCKET), diagnostics.namespace());
    assertTrue(diagnostics.id().isPresent());
  }

  @Test
  void completesRequestsWhoseResponsesArriveOutOfOrder() throws Exception {
    start();
    startServer();
    List<FakeKeyValueServer.Packet> gets = new ArrayList<>();
    server.handler((connection, request) -> {
      if (!request.is(Opcode.GET)) {
        connection.respond(request, STATUS_SUCCESS);
        return;
      }
      synchronized (gets) {
        gets.add(request);
        if (gets.size() == 2) {
          // Answer the second one first.
          for (int i = 1; i >= 0; i--) {
            FakeKeyValueServer.Packet get = gets.get(i);
            String key = get.key().substring(1); // after the collection ID (one byte, for the default collection)
            connection.respond(get, STATUS_SUCCESS, ("value of " + key).getBytes(UTF_8));
          }
        }
      }
    });
    SocketKeyValueEndpoint endpoint = connectedEndpoint();

    GetRequest first = newGetRequest("first");
    GetRequest second = newGetRequest("second");
    endpoint.send(first);
    endpoint.send(second);

    GetResponse firstResponse = first.response().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    GetResponse secondResponse = second.response().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    assertArrayEquals("value of first".getBytes(UTF_8), firstResponse.content());
    assertArrayEquals("value of second".getBytes(UTF_8), secondResponse.content());
    assertTrue(first.context().dispatchLatency() > 0);
    assertTrue(endpoint.lastResponseReceived() > 0);
  }

  @Test
  void completesRequestsWhoseResponsesWereReadWithOneThatClosesTheConnection() throws Exception {
    start();
    startServer();
    List<FakeKeyValueServer.Packet> gets = new ArrayList<>();
    server.handler((connection, request) -> {
      if (!request.is(Opcode.GET)) {
        connection.respond(request, STATUS_SUCCESS);
        return;
      }
      gets.add(request);
      if (gets.size() == 2) {
        // In one write, so the client reads them together.
        connection.send(concat(
          connection.response(gets.get(0), STATUS_NO_BUCKET, new byte[0]), // closes the connection
          connection.response(gets.get(1), STATUS_SUCCESS, "value".getBytes(UTF_8))
        ));
      }
    });
    SocketKeyValueEndpoint endpoint = connectedEndpoint();

    // Fail fast, so they're canceled if they're retried (when the connection closes).
    GetRequest first = newGetRequest("first", FailFastRetryStrategy.INSTANCE);
    GetRequest second = newGetRequest("second", FailFastRetryStrategy.INSTANCE);
    endpoint.send(first);
    endpoint.send(second);

    ExecutionException e = assertThrows(ExecutionException.class, () -> first.response().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
    assertInstanceOf(RequestCanceledException.class, e.getCause());
    GetResponse secondResponse = second.response().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    assertArrayEquals("value".getBytes(UTF_8), secondResponse.content());

    waitForEvent(UnexpectedEndpointDisconnectedEvent.class);
    assertEquals(1, events(ChannelClosedProactivelyEvent.class).size());
  }

  private static byte[] concat(byte[]... arrays) {
    ByteArrayOutputStream result = new ByteArrayOutputStream();
    for (byte[] array : arrays) {
      result.write(array, 0, array.length);
    }
    return result.toByteArray();
  }

  /**
   * Requests sent by a response callback run on the reader thread, which defers waking the writer until it has
   * handled every response it already read. Checks that those requests are always written.
   */
  /**
   * A request sent from a response callback, while the reader waits for the rest of a packet, waits at most about
   * a second (the reader only wakes the writer once it has the whole packet). Even with idle detection off.
   */
  @Test
  void writesRequestsSentFromResponseCallbacksWhileWaitingForTheRestOfAPacket() throws Exception {
    start(env -> env.ioConfig(it -> it.configIdleRedialTimeout(Duration.ZERO)));
    startServer();
    // Answers the first GET with the second GET's response, but only the first half of the second one's.
    // Sends the rest once it has the third GET (sent from the first response's callback).
    AtomicInteger gets = new AtomicInteger();
    List<FakeKeyValueServer.Packet> held = new CopyOnWriteArrayList<>();
    AtomicReference<byte[]> restOfSecond = new AtomicReference<>();
    server.handler((connection, request) -> {
      if (!request.is(Opcode.GET)) {
        connection.respond(request, STATUS_SUCCESS);
        return;
      }
      switch (gets.incrementAndGet()) {
        case 1:
          held.add(request);
          break;
        case 2:
          byte[] first = connection.response(held.get(0), STATUS_SUCCESS, new byte[0]);
          byte[] second = connection.response(request, STATUS_SUCCESS, new byte[100]);
          int half = second.length / 2;
          restOfSecond.set(Arrays.copyOfRange(second, half, second.length));
          byte[] firstAndHalf = Arrays.copyOf(first, first.length + half); // in one write, so they're read together
          System.arraycopy(second, 0, firstAndHalf, first.length, half);
          connection.send(firstAndHalf);
          break;
        default:
          connection.send(restOfSecond.get());
          connection.respond(request, STATUS_SUCCESS);
      }
    });
    SocketKeyValueEndpoint endpoint = connectedEndpoint();

    GetRequest third = newGetRequest("baz");
    GetRequest first = newGetRequest("foo");
    first.response().thenRun(() -> endpoint.send(third));
    endpoint.send(first);
    GetRequest second = newGetRequest("bar");
    endpoint.send(second);

    third.response().get(5, TimeUnit.SECONDS);
    second.response().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
  }

  @Test
  void writesRequestsSentFromResponseCallbacks() throws Exception {
    start();
    startServer();
    SocketKeyValueEndpoint endpoint = connectedEndpoint();

    for (int chains : new int[]{1, 64}) {
      int perChain = 200;
      CountDownLatch done = new CountDownLatch(chains);
      List<Throwable> failures = new CopyOnWriteArrayList<>();
      for (int i = 0; i < chains; i++) {
        sendChain(endpoint, perChain, done, failures);
      }
      assertTrue(done.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS), "chains didn't finish; requests were left unwritten");
      assertTrue(failures.isEmpty(), failures.toString());
    }
  }

  /**
   * Like an application using the blocking API with a virtual thread per request:
   * many virtual threads sending at once, each waiting for its response before sending the next.
   */
  @Test
  void completesRequestsSentFromManyVirtualThreads() throws Exception {
    ThreadFactory virtualThreads = CbThreads.virtualThreadFactoryOrNull("test-sender-");
    assumeTrue(virtualThreads != null, "needs virtual threads (Java 21+)");
    start();
    startServer();
    server.handler((connection, request) -> connection.respond(request, STATUS_SUCCESS, "v".getBytes(UTF_8)));
    SocketKeyValueEndpoint endpoint = connectedEndpoint();

    int threads = 64;
    int perThread = 200;
    CountDownLatch done = new CountDownLatch(threads);
    List<Throwable> failures = new CopyOnWriteArrayList<>();
    for (int i = 0; i < threads; i++) {
      virtualThreads.newThread(() -> {
        try {
          for (int j = 0; j < perThread; j++) {
            GetRequest request = newGetRequest("key");
            endpoint.send(request);
            request.response().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
          }
        } catch (Throwable t) {
          failures.add(t);
        } finally {
          done.countDown();
        }
      }).start();
    }

    assertTrue(done.await(TIMEOUT.toMillis() * 2, TimeUnit.MILLISECONDS), "senders didn't finish");
    assertTrue(failures.isEmpty(), failures.toString());
  }

  private void sendChain(SocketKeyValueEndpoint endpoint, int remaining, CountDownLatch done, List<Throwable> failures) {
    GetRequest request = newGetRequest("chain");
    request.response().whenComplete((response, t) -> {
      if (t != null) {
        failures.add(t);
        done.countDown();
      } else if (remaining == 1) {
        done.countDown();
      } else {
        sendChain(endpoint, remaining - 1, done, failures); // on the reader thread
      }
    });
    endpoint.send(request);
  }

  @Test
  void retriesRequestsInFlightAndReconnectsWhenServerClosesConnection() throws Exception {
    start();
    startServer();
    server.handler((connection, request) -> {
      if (request.is(Opcode.GET)) {
        connection.close(); // instead of answering
      } else {
        connection.respond(request, STATUS_SUCCESS);
      }
    });
    SocketKeyValueEndpoint endpoint = connectedEndpoint();
    String firstChannelId = endpoint.context().channelId().orElseThrow(AssertionError::new);

    GetRequest request = newGetRequest("foo", FailFastRetryStrategy.INSTANCE);
    endpoint.send(request);

    ExecutionException e = assertThrows(ExecutionException.class, () -> request.response().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
    RequestCanceledException canceled = assertInstanceOf(RequestCanceledException.class, e.getCause());
    assertTrue(canceled.getMessage().contains(RetryReason.CHANNEL_CLOSED_WHILE_IN_FLIGHT.name()), canceled.getMessage());

    UnexpectedEndpointDisconnectedEvent disconnected = waitForEvent(UnexpectedEndpointDisconnectedEvent.class);
    assertEquals(Event.Severity.WARN, disconnected.severity());

    // Reconnects.
    waitUntil(() -> endpoint.state() == EndpointState.CONNECTED
      && !endpoint.context().channelId().orElse("").equals(firstChannelId));

    // Like the Netty implementation, the server closing the connection isn't reported as a failure.
    assertTrue(events(GenericFailureDetectedEvent.class).isEmpty());
  }

  @Test
  void reportsWhyTheConnectionFailed() throws Exception {
    start();
    startServer();
    server.handler((connection, request) -> {
      if (request.is(Opcode.GET)) {
        connection.socket.setSoLinger(true, 0); // so closing resets the connection
        connection.close();
      } else {
        connection.respond(request, STATUS_SUCCESS);
      }
    });
    SocketKeyValueEndpoint endpoint = connectedEndpoint();

    endpoint.send(newGetRequest("foo", FailFastRetryStrategy.INSTANCE));

    GenericFailureDetectedEvent failure = waitForEvent(GenericFailureDetectedEvent.class);
    assertInstanceOf(SocketException.class, failure.cause());
    waitForEvent(UnexpectedEndpointDisconnectedEvent.class);
    assertEquals(1, events(GenericFailureDetectedEvent.class).size());
  }

  @Test
  void retriesWhenNotMyVbucketAndProposesTheConfigInTheResponse() throws Exception {
    start();
    startServer();
    String config = "{\"rev\":1234}";
    server.handler((connection, request) -> {
      if (request.is(Opcode.GET)) {
        connection.respond(request, STATUS_NOT_MY_VBUCKET, config.getBytes(UTF_8));
      } else {
        connection.respond(request, STATUS_SUCCESS);
      }
    });
    SocketKeyValueEndpoint endpoint = connectedEndpoint();

    GetRequest request = newGetRequest("foo");
    endpoint.send(request);

    waitUntil(() -> request.context().retryReasons() != null
      && request.context().retryReasons().contains(RetryReason.KV_NOT_MY_VBUCKET));
    assertEquals(1, request.rejectedWithNotMyVbucket());
    verify(configurationProvider, timeout(TIMEOUT.toMillis())).proposeBucketConfig(any(ProposedBucketConfigContext.class));
  }

  @Test
  void failsToConnectWhenAuthenticationFails() throws Exception {
    start();
    startServer();
    SocketKeyValueEndpoint endpoint = newEndpoint(plainAuthenticator("wrong password"), server.port());
    endpoint.connect();

    EndpointConnectionFailedEvent failed = waitForEvent(EndpointConnectionFailedEvent.class);
    AuthenticationFailureException e = assertInstanceOf(AuthenticationFailureException.class, failed.cause());
    assertTrue(e.getMessage().contains("User authentication failed"), e.getMessage());
    assertInstanceOf(AuthenticationFailureException.class, endpoint.lastConnectAttemptFailure());
    assertEquals(AuthenticationStatus.FAILED, endpoint.internalDiagnostics().authenticationStatus);
    assertTrue(endpoint.state() == EndpointState.CONNECTING || endpoint.state() == EndpointState.DISCONNECTED);

    // Keeps trying, and connects once it can.
    server.expectedPassword = "wrong password";
    waitUntil(() -> endpoint.state() == EndpointState.CONNECTED);
  }

  @Test
  void restartsAuthenticationWithMechanismsBothSupport() throws Exception {
    start();
    startServer();
    // The client's first choice (SCRAM-SHA512) isn't one the server lists, so it tries again with PLAIN.
    Authenticator authenticator = PasswordAuthenticator.builder(USERNAME, PASSWORD)
      .allowedSaslMechanisms(EnumSet.of(SaslMechanism.PLAIN, SaslMechanism.SCRAM_SHA512))
      .build();
    server.saslMechanisms = "PLAIN";
    server.handler((connection, request) -> connection.respond(request, STATUS_SUCCESS));

    SocketKeyValueEndpoint endpoint = newEndpoint(authenticator, server.port());
    endpoint.connect();
    waitUntil(() -> endpoint.state() == EndpointState.CONNECTED);

    List<String> mechanisms = server.receivedSoFar().stream()
      .filter(it -> it.is(Opcode.SASL_AUTH))
      .map(FakeKeyValueServer.Packet::key)
      .collect(Collectors.toList());
    assertEquals(listOf("SCRAM-SHA512", "PLAIN"), mechanisms);
  }

  @Test
  void failsToConnectWhenBucketDoesNotExist() throws Exception {
    start();
    startServer();
    server.selectBucketStatus = STATUS_NOT_FOUND;
    SocketKeyValueEndpoint endpoint = newEndpoint();
    endpoint.connect();

    EndpointConnectionFailedEvent failed = waitForEvent(EndpointConnectionFailedEvent.class);
    assertInstanceOf(BucketNotFoundException.class, failed.cause());
    assertEquals(Event.Severity.DEBUG, failed.severity());
  }

  @Test
  void handshakeTimesOutWhenServerDoesNotAnswer() throws Exception {
    start(env -> env.timeoutConfig(it -> it.connectTimeout(Duration.ofMillis(300))));
    startServer();
    server.answerHandshake = false;
    SocketKeyValueEndpoint endpoint = newEndpoint();
    endpoint.connect();

    EndpointConnectionFailedEvent failed = waitForEvent(EndpointConnectionFailedEvent.class);
    TimeoutException e = assertInstanceOf(TimeoutException.class, failed.cause());
    assertTrue(e.getMessage().contains("KV Feature Negotiation timed out"), e.getMessage());
    assertEquals(Duration.ofMillis(300), failed.duration());
  }

  /**
   * The socket has no read timeout during the TLS handshake (so it stays blocking); the connect deadline still applies.
   */
  @Test
  void tlsHandshakeTimesOutWhenServerDoesNotAnswer() throws Exception {
    HeldCertificate cert = new HeldCertificate.Builder().addSubjectAlternativeName(HOST).build();
    start(env -> env
      .timeoutConfig(it -> it.connectTimeout(Duration.ofMillis(300)))
      .securityConfig(it -> it.enableTls(true).trustCertificates(listOf(cert.certificate()))));
    // Accepts connections, but never reads or writes (so never answers the ClientHello).
    ServerSocket silent = new ServerSocket(0, 50, InetAddress.getByName(HOST));
    List<Socket> accepted = new CopyOnWriteArrayList<>();
    Thread acceptor = new Thread(() -> {
      try {
        while (true) {
          accepted.add(silent.accept());
        }
      } catch (IOException e) {
        // closed
      }
    });
    acceptor.setDaemon(true);
    acceptor.start();
    cleanup.add(0, () -> {
      silent.close();
      for (Socket s : accepted) {
        s.close();
      }
    });

    SocketKeyValueEndpoint endpoint = newEndpoint(plainAuthenticator(PASSWORD), silent.getLocalPort());
    endpoint.connect();

    EndpointConnectionFailedEvent failed = waitForEvent(EndpointConnectionFailedEvent.class);
    SocketTimeoutException e = assertInstanceOf(SocketTimeoutException.class, failed.cause());
    assertEquals("Read timed out", e.getMessage());
    assertTrue(failed.duration().compareTo(Duration.ofMillis(250)) >= 0 && failed.duration().compareTo(Duration.ofSeconds(5)) < 0,
      failed.duration().toString());
    assertTrue(events(SecureConnectionFailedEvent.class).isEmpty(), "a timeout isn't a TLS failure");
  }

  /**
   * Like a server that's restarting. Like the Netty implementation, records the handshake failure,
   * but doesn't treat it as a TLS (or authentication) failure, since it might be temporary.
   */
  @Test
  void serverClosingTheConnectionDuringTlsHandshakeIsNotATlsFailure() throws Exception {
    HeldCertificate cert = new HeldCertificate.Builder().addSubjectAlternativeName(HOST).build();
    start(env -> env.securityConfig(it -> it.enableTls(true).trustCertificates(listOf(cert.certificate()))));
    // Reads the ClientHello, then closes the connection (cleanly: closing with unread bytes would reset it).
    ServerSocket closing = new ServerSocket(0, 50, InetAddress.getByName(HOST));
    Thread acceptor = new Thread(() -> {
      try {
        while (true) {
          try (Socket s = closing.accept()) {
            DataInputStream in = new DataInputStream(s.getInputStream());
            byte[] recordHeader = new byte[5];
            in.readFully(recordHeader);
            in.readFully(new byte[((recordHeader[3] & 0xff) << 8) | (recordHeader[4] & 0xff)]);
          }
        }
      } catch (IOException e) {
        // closed
      }
    });
    acceptor.setDaemon(true);
    acceptor.start();
    cleanup.add(0, closing::close);

    SocketKeyValueEndpoint endpoint = newEndpoint(plainAuthenticator(PASSWORD), closing.getLocalPort());
    endpoint.connect();

    EndpointConnectionFailedEvent failed = waitForEvent(EndpointConnectionFailedEvent.class);
    assertInstanceOf(SSLException.class, failed.cause());
    assertNotNull(endpoint.internalDiagnostics().tlsHandshakeFailure);
    assertNotEquals(AuthenticationStatus.FAILED, endpoint.internalDiagnostics().authenticationStatus);
    assertTrue(events(SecureConnectionFailedEvent.class).isEmpty());
  }

  @Test
  void connectFailureIsAnnotated() throws Exception {
    start();
    startServer();
    int closedPort = server.port();
    server.close();

    SocketKeyValueEndpoint endpoint = newEndpoint(plainAuthenticator(PASSWORD), closedPort);
    endpoint.connect();

    EndpointConnectionFailedEvent failed = waitForEvent(EndpointConnectionFailedEvent.class);
    assertTrue(failed.cause().getMessage().contains("Check server ports and cluster encryption setting."), failed.cause().getMessage());
  }

  @Test
  void disconnectClosesTheConnection() throws Exception {
    start();
    startServer();
    SocketKeyValueEndpoint endpoint = connectedEndpoint();
    FakeKeyValueServer.Connection connection = server.nextConnection(TIMEOUT);

    endpoint.disconnect();

    waitUntil(() -> endpoint.state() == EndpointState.DISCONNECTED);
    assertTrue(endpoint.receivedDisconnectSignal());
    // The server sees the connection close (and closes its end).
    waitUntil(connection.socket::isClosed);
  }

  @Test
  void failureAfterOpeningIsAFailedAttempt() throws Exception {
    start();
    startServer();
    // For example, a custom event bus that throws.
    RuntimeException failure = new RuntimeException("event bus failed");
    AtomicBoolean failed = new AtomicBoolean();
    onPublish = event -> {
      if (event instanceof EndpointConnectedEvent && failed.compareAndSet(false, true)) {
        throw failure;
      }
    };
    SocketKeyValueEndpoint endpoint = newEndpoint();
    endpoint.connect();

    // Tries again, instead of being left CONNECTING.
    EndpointConnectionFailedEvent attemptFailed = waitForEvent(EndpointConnectionFailedEvent.class);
    assertSame(failure, attemptFailed.cause());
    waitUntil(() -> endpoint.state() == EndpointState.CONNECTED);
    FakeKeyValueServer.Connection first = server.nextConnection(TIMEOUT);
    server.nextConnection(TIMEOUT);
    waitUntil(first.socket::isClosed); // the server's side closes when the endpoint closes its own

    GetRequest request = newGetRequest("foo");
    endpoint.send(request);
    request.response().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
  }

  @Test
  void disconnectWhileHandlingUnexpectedCloseDoesNotReconnect() throws Exception {
    start();
    startServer();
    SocketKeyValueEndpoint endpoint = connectedEndpoint();
    FakeKeyValueServer.Connection connection = server.nextConnection(TIMEOUT);

    // Disconnects after the endpoint decides to reconnect, but before it does.
    onPublish = event -> {
      if (event instanceof UnexpectedEndpointDisconnectedEvent) {
        endpoint.disconnect();
      }
    };
    connection.close();

    waitForEvent(UnexpectedEndpointDisconnectedEvent.class);
    waitUntil(() -> endpoint.state() == EndpointState.DISCONNECTED);
    Thread.sleep(200);
    assertEquals(EndpointState.DISCONNECTED, endpoint.state());
  }

  @Test
  void disconnectStopsConnecting() throws Exception {
    start();
    startServer();
    server.answerHandshake = false;
    SocketKeyValueEndpoint endpoint = newEndpoint();
    endpoint.connect();
    server.nextConnection(TIMEOUT);

    endpoint.disconnect();

    waitUntil(() -> endpoint.state() == EndpointState.DISCONNECTED);
    Thread.sleep(200);
    assertEquals(EndpointState.DISCONNECTED, endpoint.state());
  }

  @Test
  void handlesClustermapChangeNotification() throws Exception {
    start();
    startServer();
    connectedEndpoint();
    FakeKeyValueServer.Connection connection = server.nextConnection(TIMEOUT);

    connection.pushClustermapChangeNotification(BUCKET, 1, 42);

    verify(configurationProvider, timeout(TIMEOUT.toMillis())).signalNewTopologyAvailable(
      eq(BUCKET), eq(new TopologyRevision(1, 42)), any()
    );
  }

  @Test
  void ignoresResponsesReadWithOneThatHasUnknownOpaque() throws Exception {
    start();
    startServer();
    server.handler((connection, request) -> {
      if (!request.is(Opcode.GET)) {
        connection.respond(request, STATUS_SUCCESS);
        return;
      }
      // In one write, so the client reads them together.
      connection.send(concat(
        connection.response(Opcode.GET.opcode(), 0xdead, STATUS_SUCCESS, new byte[0]),
        connection.response(request, STATUS_SUCCESS, "value".getBytes(UTF_8))
      ));
    });
    SocketKeyValueEndpoint endpoint = connectedEndpoint();

    // Fail fast, so it's canceled when it's retried (because the connection closed).
    GetRequest request = newGetRequest("foo", FailFastRetryStrategy.INSTANCE);
    endpoint.send(request);

    ExecutionException e = assertThrows(ExecutionException.class, () -> request.response().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
    RequestCanceledException canceled = assertInstanceOf(RequestCanceledException.class, e.getCause());
    assertTrue(canceled.getMessage().contains(RetryReason.CHANNEL_CLOSED_WHILE_IN_FLIGHT.name()), canceled.getMessage());
  }

  @Test
  void reportsJvmErrorOnTheReaderThreadAndReconnects() throws Exception {
    start();
    startServer();
    SocketKeyValueEndpoint endpoint = connectedEndpoint();
    String channelId = endpoint.context().channelId().orElseThrow(AssertionError::new);
    FakeKeyValueServer.Connection connection = server.nextConnection(TIMEOUT);
    // Thrown on the reader thread, while it handles the response. (Also logged at ERROR.)
    OutOfMemoryError error = new OutOfMemoryError("simulated");
    onPublish = event -> {
      if (event instanceof UnknownResponseReceivedEvent) {
        throw error;
      }
    };

    connection.respond(Opcode.GET.opcode(), 0xdead, STATUS_SUCCESS, new byte[0]);

    GenericFailureDetectedEvent failure = waitForEvent(GenericFailureDetectedEvent.class);
    assertSame(error, failure.cause());
    waitForEvent(UnexpectedEndpointDisconnectedEvent.class);
    waitUntil(() -> endpoint.state() == EndpointState.CONNECTED
      && !endpoint.context().channelId().orElse("").equals(channelId));
  }

  @Test
  void closesConnectionWhenResponseHasUnknownOpaque() throws Exception {
    start();
    startServer();
    SocketKeyValueEndpoint endpoint = connectedEndpoint();
    FakeKeyValueServer.Connection connection = server.nextConnection(TIMEOUT);

    connection.respond(Opcode.GET.opcode(), 0xdead, STATUS_SUCCESS, new byte[0]);

    waitForEvent(UnknownResponseReceivedEvent.class);
    ChannelClosedProactivelyEvent closed = waitForEvent(ChannelClosedProactivelyEvent.class);
    assertEquals(ChannelClosedProactivelyEvent.Reason.KV_RESPONSE_CONTAINED_UNKNOWN_OPAQUE, closed.reason());
    waitForEvent(UnexpectedEndpointDisconnectedEvent.class);
    waitUntil(() -> endpoint.state() == EndpointState.CONNECTED);
  }

  @Test
  void sendsNoopWhenIdleAndClosesConnectionWhenServerStaysQuiet() throws Exception {
    start(env -> env.ioConfig(it -> it.configIdleRedialTimeout(Duration.ofMillis(400))));
    startServer();
    server.handler((connection, request) -> {
      // Doesn't answer the NOOPs.
    });
    connectedEndpoint();

    // Two unanswered NOOPs (see KeyValueIdleDetectorTest for the exact timing).
    server.nextReceived(Opcode.NOOP, TIMEOUT);
    server.nextReceived(Opcode.NOOP, TIMEOUT);
    waitForEvent(UnexpectedEndpointDisconnectedEvent.class);
  }

  @Test
  void failsOnlyTheRequestWhenTheTracerThrows() throws Exception {
    start();
    startServer();
    IllegalStateException tracerFailure = new IllegalStateException("broken tracer");
    RequestTracer brokenTracer = new RequestTracer() {
      @Override
      public RequestSpan requestSpan(String name, RequestSpan parent) {
        throw tracerFailure;
      }

      @Override
      public Mono<Void> start() {
        return Mono.empty();
      }

      @Override
      public Mono<Void> stop(Duration timeout) {
        return Mono.empty();
      }
    };
    CoreResources resources = core.coreResources();
    when(core.coreResources()).thenReturn(new CoreResources(brokenTracer, resources.tracingDecorator(), resources.meter()));
    SocketKeyValueEndpoint endpoint = connectedEndpoint();
    String channelId = endpoint.context().channelId().orElseThrow(AssertionError::new);

    GetRequest traced = new GetRequest("foo", TIMEOUT, core.context(), DEFAULT_COLLECTION, BestEffortRetryStrategy.INSTANCE, NoopRequestSpan.INSTANCE);
    endpoint.send(traced);
    ExecutionException e = assertThrows(ExecutionException.class, () -> traced.response().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
    assertSame(tracerFailure, e.getCause());

    // The connection is still open, and works for requests without spans.
    GetRequest untraced = newGetRequest("bar");
    endpoint.send(untraced);
    untraced.response().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    assertEquals(EndpointState.CONNECTED, endpoint.state());
    assertEquals(channelId, endpoint.context().channelId().orElse(null));
    assertTrue(events(UnexpectedEndpointDisconnectedEvent.class).isEmpty());
  }

  @Test
  void closesConnectionWhenServerStaysQuietWhileWritesAreStuck() throws Exception {
    start(env -> env.ioConfig(it -> it
      .configIdleRedialTimeout(Duration.ofMillis(400))
      .highWaterMark(StorageSize.ofMebibytes(64))));
    startServer();
    CountDownLatch released = new CountDownLatch(1);
    cleanup.add(0, released::countDown);
    server.handler((connection, request) -> {
      if (request.is(Opcode.GET)) {
        // Stops reading (and answering), so the client's writer blocks.
        try {
          released.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        return;
      }
      connection.respond(request, STATUS_SUCCESS);
    });
    SocketKeyValueEndpoint endpoint = connectedEndpoint();

    // Far more than the sockets' buffers hold.
    String bigKey = new String(new char[200]).replace('\0', 'x');
    for (int i = 0; i < 100_000; i++) {
      endpoint.send(newGetRequest(bigKey + i));
    }

    waitForEvent(UnexpectedEndpointDisconnectedEvent.class);
  }

  @ParameterizedTest
  @ValueSource(longs = {0, -1000})
  void noIdleDetectionWhenIdleTimeoutIsNotPositive(long idleTimeoutMillis) throws Exception {
    // Like the Netty implementation, where a timeout that's not positive turns off its IdleStateHandler.
    start(env -> env.ioConfig(it -> it.configIdleRedialTimeout(Duration.ofMillis(idleTimeoutMillis))));
    startServer();
    SocketKeyValueEndpoint endpoint = connectedEndpoint();
    String channelId = endpoint.context().channelId().orElseThrow(AssertionError::new);

    Thread.sleep(300);

    assertTrue(
      server.receivedSoFar().stream().noneMatch(it -> it.is(Opcode.NOOP)),
      "should not send NOOPs"
    );
    assertTrue(events(UnexpectedEndpointDisconnectedEvent.class).isEmpty());
    assertEquals(channelId, endpoint.context().channelId().orElse(null));

    GetRequest request = newGetRequest("foo");
    endpoint.send(request);
    request.response().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
  }

  @Test
  void connectionAnsweringNoopsStaysOpen() throws Exception {
    start(env -> env.ioConfig(it -> it.configIdleRedialTimeout(Duration.ofMillis(400))));
    startServer();
    SocketKeyValueEndpoint endpoint = connectedEndpoint();
    String channelId = endpoint.context().channelId().orElseThrow(AssertionError::new);

    // Well past the idle timeout.
    for (int i = 0; i < 5; i++) {
      server.nextReceived(Opcode.NOOP, TIMEOUT);
    }
    assertTrue(events(UnexpectedEndpointDisconnectedEvent.class).isEmpty());
    assertEquals(channelId, endpoint.context().channelId().orElse(null));
  }

  @Test
  void reauthenticatesOnceConnectedWhenTriggeredDuringTheHandshake() throws Exception {
    start();
    startServer();
    String jwt = base64Url("{}") + "." + base64Url("{}") + "." + base64Url("signature");
    when(core.context().authenticator()).thenReturn(JwtAuthenticator.create(jwt));
    // Published by the handshake, after HELLO.
    onPublish = event -> {
      if (event instanceof FeaturesNegotiatedEvent) {
        authenticationRefreshTriggers.publish(Core.AuthenticationRefreshTrigger.INSTANCE);
      }
    };
    connectedEndpoint();
    server.nextReceived(Opcode.SASL_AUTH, TIMEOUT); // the handshake's

    FakeKeyValueServer.Packet auth = server.nextReceived(Opcode.SASL_AUTH, TIMEOUT);
    assertEquals("OAUTHBEARER", auth.key());
  }

  @Test
  void reauthenticatesWhenTriggered() throws Exception {
    start();
    startServer();
    // Reauthenticates with the Core's authenticator, which supports single-step authentication.
    String jwt = base64Url("{}") + "." + base64Url("{}") + "." + base64Url("signature");
    when(core.context().authenticator()).thenReturn(JwtAuthenticator.create(jwt));
    connectedEndpoint();
    server.nextReceived(Opcode.SASL_AUTH, TIMEOUT); // the handshake's

    authenticationRefreshTriggers.publish(Core.AuthenticationRefreshTrigger.INSTANCE);

    FakeKeyValueServer.Packet auth = server.nextReceived(Opcode.SASL_AUTH, TIMEOUT);
    assertEquals("OAUTHBEARER", auth.key());
    assertTrue(new String(auth.body(), UTF_8).contains("auth=Bearer " + jwt));

    // The fake server rejects it, so the connection closes (and the endpoint reconnects).
    waitForEvent(UnexpectedEndpointDisconnectedEvent.class);
  }

  @Test
  void stopsAcceptingRequestsWhenTooMuchIsWaitingToBeWritten() throws Exception {
    start(env -> env.ioConfig(it -> it
      .lowWaterMark(StorageSize.ofBytes(1024))
      .highWaterMark(StorageSize.ofBytes(4 * 1024))));
    startServer();
    CountDownLatch released = new CountDownLatch(1);
    cleanup.add(0, released::countDown);
    server.handler((connection, request) -> {
      if (request.is(Opcode.GET)) {
        // Stops reading, so the client's writes back up.
        try {
          released.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        return;
      }
      connection.respond(request, STATUS_SUCCESS);
    });
    SocketKeyValueEndpoint endpoint = connectedEndpoint();

    // Big keys, so the socket's buffers fill up quickly.
    String bigKey = new String(new char[200]).replace('\0', 'x');
    List<GetRequest> requests = new ArrayList<>();
    for (int i = 0; i < 100_000 && requests.stream().noneMatch(it -> it.context().retryReasons() != null); i++) {
      GetRequest request = newGetRequest(bigKey + i);
      requests.add(request);
      endpoint.send(request);
    }

    Set<RetryReason> reasons = requests.stream()
      .filter(it -> it.context().retryReasons() != null)
      .flatMap(it -> it.context().retryReasons().stream())
      .collect(Collectors.toSet());
    assertEquals(EnumSet.of(RetryReason.ENDPOINT_NOT_WRITABLE), reasons);
  }

  @Test
  void connectsWithTls() throws Exception {
    HeldCertificate serverCert = new HeldCertificate.Builder().addSubjectAlternativeName(HOST).build();
    HandshakeCertificates serverCerts = new HandshakeCertificates.Builder().heldCertificate(serverCert).build();
    start(env -> env.securityConfig(it -> it.enableTls(true).trustCertificates(listOf(serverCert.certificate()))));
    server = FakeKeyValueServer.start(serverCerts.sslContext().getServerSocketFactory());
    cleanup.add(0, server);
    server.handler((connection, request) -> connection.respond(request, STATUS_SUCCESS, "hello".getBytes(UTF_8)));

    SocketKeyValueEndpoint endpoint = connectedEndpoint();
    GetRequest request = newGetRequest("foo");
    endpoint.send(request);

    GetResponse response = request.response().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    assertEquals(ResponseStatus.SUCCESS, response.status());
    assertArrayEquals("hello".getBytes(UTF_8), response.content());
    assertEquals(null, endpoint.internalDiagnostics().tlsHandshakeFailure);
  }

  @Test
  void usesTheConfiguredCipherSuites() throws Exception {
    HeldCertificate serverCert = new HeldCertificate.Builder().addSubjectAlternativeName(HOST).build();
    HandshakeCertificates serverCerts = new HandshakeCertificates.Builder().heldCertificate(serverCert).build();
    List<String> ciphers = listOf("TLS_AES_256_GCM_SHA384");
    start(env -> env.securityConfig(it -> it.enableTls(true).trustCertificates(listOf(serverCert.certificate())).ciphers(ciphers)));
    server = FakeKeyValueServer.start(serverCerts.sslContext().getServerSocketFactory());
    cleanup.add(0, server);
    List<String> negotiated = new CopyOnWriteArrayList<>();
    server.handler((connection, request) -> {
      negotiated.add(((SSLSocket) connection.socket).getSession().getCipherSuite());
      connection.respond(request, STATUS_SUCCESS);
    });

    SocketKeyValueEndpoint endpoint = connectedEndpoint();

    assertEquals(ciphers, waitForEvent(CustomTlsCiphersEnabledEvent.class).ciphers());
    GetRequest request = newGetRequest("foo");
    endpoint.send(request);
    request.response().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    assertEquals("TLS_AES_256_GCM_SHA384", negotiated.get(0));
  }

  @Test
  void untrustedServerCertificateFailsToConnect() throws Exception {
    HeldCertificate trusted = new HeldCertificate.Builder().addSubjectAlternativeName(HOST).build();
    HeldCertificate untrusted = new HeldCertificate.Builder().addSubjectAlternativeName(HOST).build();
    HandshakeCertificates serverCerts = new HandshakeCertificates.Builder().heldCertificate(untrusted).build();
    start(env -> env.securityConfig(it -> it.enableTls(true).trustCertificates(listOf(trusted.certificate()))));
    server = FakeKeyValueServer.start(serverCerts.sslContext().getServerSocketFactory());
    cleanup.add(0, server);

    SocketKeyValueEndpoint endpoint = newEndpoint();
    endpoint.connect();

    SecureConnectionFailedEvent tlsFailed = waitForEvent(SecureConnectionFailedEvent.class);
    assertNotNull(tlsFailed);
    EndpointConnectionFailedEvent failed = waitForEvent(EndpointConnectionFailedEvent.class);
    assertTrue(failed.cause().getMessage().contains(CERTIFICATE_HINT), failed.cause().getMessage());
    assertNotNull(endpoint.internalDiagnostics().tlsHandshakeFailure);
    assertEquals(AuthenticationStatus.FAILED, endpoint.internalDiagnostics().authenticationStatus);
    assertFalse(endpoint.state() == EndpointState.CONNECTED);
  }

  @Test
  void rejectedClientCertificateFailsToConnect() throws Exception {
    HeldCertificate serverCert = new HeldCertificate.Builder().addSubjectAlternativeName(HOST).build();
    HandshakeCertificates serverCerts = new HandshakeCertificates.Builder().heldCertificate(serverCert).build();
    start(env -> env.securityConfig(it -> it.enableTls(true).trustCertificates(listOf(serverCert.certificate()))));
    // Like a server that requires a client certificate. With TLS 1.3, the client finishes its side of the TLS
    // handshake before the server rejects its (missing) certificate, so the alert arrives during the KV handshake.
    // (Newer JDKs send "certificate_required", and older ones "bad_certificate".)
    SSLServerSocket serverSocket = (SSLServerSocket) serverCerts.sslContext().getServerSocketFactory()
      .createServerSocket(0, 50, InetAddress.getByName(HOST));
    serverSocket.setEnabledProtocols(new String[]{"TLSv1.3"});
    serverSocket.setNeedClientAuth(true);
    server = FakeKeyValueServer.start(serverSocket);
    cleanup.add(0, server);

    SocketKeyValueEndpoint endpoint = newEndpoint();
    endpoint.connect();

    waitForEvent(SecureConnectionFailedEvent.class);
    EndpointConnectionFailedEvent failed = waitForEvent(EndpointConnectionFailedEvent.class);
    String message = failed.cause().getMessage();
    assertTrue(message.contains(CLIENT_CERTIFICATE_REQUIRED_HINT) || message.contains(CLIENT_CERTIFICATE_HINT), message);
    assertNotNull(endpoint.internalDiagnostics().tlsHandshakeFailure);
    assertEquals(AuthenticationStatus.FAILED, endpoint.internalDiagnostics().authenticationStatus);
    assertFalse(endpoint.state() == EndpointState.CONNECTED);
  }

  @Test
  void capturesTrafficWhenEnabled() throws Exception {
    start(env -> env.ioConfig(it -> it.captureTraffic(ServiceType.KV)));
    startServer();
    SocketKeyValueEndpoint endpoint = connectedEndpoint();

    GetRequest request = newGetRequest("captured-key");
    endpoint.send(request);
    request.response().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

    // Like the Netty implementation's TrafficCaptureHandler: every packet, handshake included.
    List<String> written = events(WriteTrafficCapturedEvent.class).stream().map(Event::description).collect(Collectors.toList());
    List<String> read = events(ReadTrafficCapturedEvent.class).stream().map(Event::description).collect(Collectors.toList());
    assertTrue(written.stream().anyMatch(it -> it.contains("(HELLO)")), String.join("", written));
    assertTrue(read.stream().anyMatch(it -> it.contains("(HELLO)")), String.join("", read));
    assertTrue(written.stream().anyMatch(it -> it.contains("(GET)") && it.contains("captured-key")), String.join("", written));
    assertTrue(read.stream().anyMatch(it -> it.contains("(GET)") && it.contains("(SUCCESS)")), String.join("", read));

    // The credentials are masked.
    String saslAuth = written.stream().filter(it -> it.contains("(SASL_AUTH)")).findFirst().orElseThrow(AssertionError::new);
    assertTrue(saslAuth.contains("(Hex dump omitted for SASL packet)"), saslAuth);
    assertTrue(written.stream().noneMatch(it -> it.contains(PASSWORD)), "the password should not be captured");
  }

  @Test
  void doesNotCaptureTrafficByDefault() throws Exception {
    start();
    startServer();
    SocketKeyValueEndpoint endpoint = connectedEndpoint();

    GetRequest request = newGetRequest("foo");
    endpoint.send(request);
    request.response().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

    assertTrue(events(WriteTrafficCapturedEvent.class).isEmpty());
    assertTrue(events(ReadTrafficCapturedEvent.class).isEmpty());
  }

  @Test
  void sendsNoopRequestsToo() throws Exception {
    start();
    startServer();
    SocketKeyValueEndpoint endpoint = connectedEndpoint();

    NoopRequest request = new NoopRequest(TIMEOUT, core.context(), BestEffortRetryStrategy.INSTANCE, DEFAULT_COLLECTION);
    endpoint.send(request);

    assertEquals(ResponseStatus.SUCCESS, request.response().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS).status());
  }

  @Test
  void failsOnlyTheRequestWhenItFailsVerification() throws Exception {
    start();
    NoopRequest malformed = new NoopRequest(TIMEOUT, core.context(), BestEffortRetryStrategy.INSTANCE, DEFAULT_COLLECTION) {
      @Override
      public ByteBuf encode(ByteBufAllocator alloc, int opaque, KeyValueChannelContext ctx) {
        return super.encode(alloc, opaque, ctx).writeByte(0); // longer than its total length says
      }
    };
    assertFailsOnlyTheRequestWhenItFailsVerification(malformed);
  }

  @Test
  void failsOnlyThePortableRequestWhenItFailsVerification() throws Exception {
    start();
    GetRequest malformed = new GetRequest("foo", TIMEOUT, core.context(), DEFAULT_COLLECTION, BestEffortRetryStrategy.INSTANCE, null) {
      @Override
      public void encodeTo(KeyValueRequestEncoder encoder) {
        // Extras too long for the header's length field.
        encoder.beginRequest(Opcode.GET).partition(partition()).extras().putBytes(new byte[256]);
      }
    };
    assertFailsOnlyTheRequestWhenItFailsVerification(malformed);
  }

  private void assertFailsOnlyTheRequestWhenItFailsVerification(KeyValueRequest<?> malformed) throws Exception {
    startServer();
    SocketKeyValueEndpoint endpoint = connectedEndpoint();
    String channelId = endpoint.context().channelId().orElseThrow(AssertionError::new);

    endpoint.send(malformed);
    ExecutionException e = assertThrows(ExecutionException.class, () -> malformed.response().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
    assertInstanceOf(EncodingFailureException.class, e.getCause());
    assertEquals(0, malformed.context().retryAttempts());
    waitForEvent(InvalidPacketDetectedEvent.class);

    // Nothing was written, so the connection is still open.
    GetRequest wellFormed = newGetRequest("bar");
    endpoint.send(wellFormed);
    wellFormed.response().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    assertEquals(EndpointState.CONNECTED, endpoint.state());
    assertEquals(channelId, endpoint.context().channelId().orElse(null));
    assertTrue(events(UnexpectedEndpointDisconnectedEvent.class).isEmpty());
  }

  private static String base64Url(String s) {
    return Base64.getUrlEncoder().withoutPadding().encodeToString(s.getBytes(UTF_8));
  }

  @SafeVarargs
  private static <T> List<T> listOf(T... items) {
    List<T> result = new ArrayList<>();
    for (T item : items) {
      result.add(item);
    }
    return result;
  }
}
