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

import com.couchbase.client.core.cnc.EventBus;
import com.couchbase.client.core.cnc.events.io.ErrorMapLoadedEvent;
import com.couchbase.client.core.cnc.events.io.ErrorMapLoadingFailedEvent;
import com.couchbase.client.core.cnc.events.io.ErrorMapUndecodableEvent;
import com.couchbase.client.core.cnc.events.io.FeaturesNegotiatedEvent;
import com.couchbase.client.core.cnc.events.io.FeaturesNegotiationFailedEvent;
import com.couchbase.client.core.cnc.events.io.SaslAuthenticationCompletedEvent;
import com.couchbase.client.core.cnc.events.io.SaslAuthenticationFailedEvent;
import com.couchbase.client.core.cnc.events.io.SaslAuthenticationRestartedEvent;
import com.couchbase.client.core.cnc.events.io.SaslMechanismsListedEvent;
import com.couchbase.client.core.cnc.events.io.SaslMechanismsListingFailedEvent;
import com.couchbase.client.core.cnc.events.io.SaslMechanismsSelectedEvent;
import com.couchbase.client.core.cnc.events.io.SelectBucketCompletedEvent;
import com.couchbase.client.core.cnc.events.io.SelectBucketFailedEvent;
import com.couchbase.client.core.cnc.events.io.UnknownSaslMechanismDetectedEvent;
import com.couchbase.client.core.cnc.events.io.UnsolicitedFeaturesReturnedEvent;
import com.couchbase.client.core.cnc.Event;
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.type.TypeReference;
import com.couchbase.client.core.diagnostics.AuthenticationStatus;
import com.couchbase.client.core.endpoint.EndpointContext;
import com.couchbase.client.core.env.SaslMechanism;
import com.couchbase.client.core.error.AuthenticationFailureException;
import com.couchbase.client.core.error.BucketNotFoundException;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.context.KeyValueIoErrorContext;
import com.couchbase.client.core.io.IoContext;
import com.couchbase.client.core.io.KeyValuePacket;
import com.couchbase.client.core.io.KeyValueRequestBuilder;
import com.couchbase.client.core.io.SectionReader;
import com.couchbase.client.core.io.netty.kv.ErrorMap;
import com.couchbase.client.core.io.netty.kv.ErrorMapLoadingHandler;
import com.couchbase.client.core.io.netty.kv.FeatureNegotiatingHandler;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol.Opcode;
import com.couchbase.client.core.io.netty.kv.SaslAuthenticationHandler;
import com.couchbase.client.core.io.netty.kv.SaslListMechanismsHandler;
import com.couchbase.client.core.io.netty.kv.SelectBucketHandler;
import com.couchbase.client.core.io.netty.kv.ServerFeature;
import com.couchbase.client.core.io.netty.kv.ServerPushHandler;
import com.couchbase.client.core.io.netty.kv.sasl.CouchbaseSaslClientFactory;
import com.couchbase.client.core.io.netty.kv.sasl.SaslCredentials;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.msg.kv.BaseKeyValueRequest;
import com.couchbase.client.core.util.Bytes;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static com.couchbase.client.core.util.CbCollections.setOf;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * The handshake of a KV connection that doesn't use Netty: what the Netty pipeline's {@link FeatureNegotiatingHandler},
 * {@link ErrorMapLoadingHandler}, {@link SaslListMechanismsHandler}, {@link SaslAuthenticationHandler} and
 * {@link SelectBucketHandler} do, with the same events and errors, but blocking, on the connecting thread.
 * <p>
 * Like the Netty handlers, it pipelines requests: it sends HELLO, GET_ERROR_MAP, SASL_LIST_MECHS and the first
 * SASL_AUTH together, then SELECT_BUCKET along with the last SASL request (or right away, without SASL).
 * Unlike them, it matches each response to its request by opaque, so it can skip the response to a SELECT_BUCKET
 * it sent before restarting SASL authentication with other mechanisms.
 * <p>
 * Not thread-safe; use once.
 */
@NullMarked
final class KeyValueHandshake implements CallbackHandler {

  /**
   * How the handshake talks to the server.
   */
  interface PacketIo {
    /**
     * Buffers a request: begins it, lets {@code build} build it (see {@link KeyValueRequestBuilder}),
     * and finishes it with the given opaque.
     */
    void write(Opcode opcode, int opaque, Consumer<KeyValueRequestBuilder> build) throws IOException;

    void flush() throws IOException;

    /**
     * Reads the next packet from the server (a response, or a request the server pushes).
     * The packet is valid only until the next read.
     *
     * @throws SocketTimeoutException if the connect deadline passes first.
     */
    KeyValuePacket read() throws IOException;
  }

  /**
   * What the handshake negotiated.
   */
  static final class Result {
    final Set<ServerFeature> features;
    final @Nullable ErrorMap errorMap;

    Result(Set<ServerFeature> features, @Nullable ErrorMap errorMap) {
      this.features = requireNonNull(features);
      this.errorMap = errorMap;
    }
  }

  /**
   * Version of the error map to ask for (same as ErrorMapLoadingHandler).
   */
  private static final short ERROR_MAP_VERSION = 2;

  private static final short STATUS_AUTH_ERROR = 0x20;
  private static final short STATUS_AUTH_CONTINUE = 0x21;

  private final EndpointContext endpointContext;
  private final IoContext ioContext;
  private final EventBus eventBus;
  private final PacketIo io;
  private final Duration connectTimeout;
  private final Set<ServerFeature> requestedFeatures;
  private final @Nullable SaslCredentials credentials;
  private final Optional<String> bucketName;
  private final String channelId;

  /**
   * How long each step took, keyed like the Netty implementation's connect timings (by handler name).
   */
  private final SortedMap<String, Duration> timings;
  private final Map<String, Long> timingStarts = new HashMap<>();

  /**
   * What the handshake is waiting for, for the timeout message (worded like the Netty handlers').
   */
  private String waitingFor = "KV Feature Negotiation";

  /**
   * Opaques of requests whose responses are skipped.
   */
  private final Set<Integer> discardedOpaques = new HashSet<>();

  private @Nullable SaslClient saslClient;
  private Set<SaslMechanism> serverMechanisms = EnumSet.noneOf(SaslMechanism.class);
  private int roundtripsToGo;
  private int saslOpaque;

  private boolean selectBucketSent;
  private int selectBucketOpaque;

  /**
   * @param ioContext the connection's context, with its local and remote addresses.
   * @param credentials null if the connection doesn't authenticate with SASL.
   * @param channelId the connection's ID, as sent in the HELLO request (and kept in the endpoint context).
   * @param timings where to record how long each step takes.
   */
  KeyValueHandshake(
    EndpointContext endpointContext,
    IoContext ioContext,
    PacketIo io,
    Set<ServerFeature> requestedFeatures,
    @Nullable SaslCredentials credentials,
    Optional<String> bucketName,
    String channelId,
    SortedMap<String, Duration> timings
  ) {
    this.endpointContext = requireNonNull(endpointContext);
    this.ioContext = requireNonNull(ioContext);
    this.eventBus = endpointContext.environment().eventBus();
    this.io = requireNonNull(io);
    this.connectTimeout = endpointContext.environment().timeoutConfig().connectTimeout();
    this.requestedFeatures = requireNonNull(requestedFeatures);
    this.credentials = credentials;
    this.bucketName = requireNonNull(bucketName);
    this.channelId = requireNonNull(channelId);
    this.timings = requireNonNull(timings);
  }

  /**
   * Does the handshake. Blocks until it's done.
   *
   * @throws TimeoutException if the connect deadline passes.
   * @throws AuthenticationFailureException if authentication or selecting the bucket fails.
   * @throws BucketNotFoundException if the bucket doesn't exist on this node.
   */
  Result run() throws IOException, TimeoutException {
    try {
      return doRun();
    } catch (SocketTimeoutException e) {
      // Like the Netty handlers' timeouts, which fail the connect attempt with a TimeoutException.
      TimeoutException timeout = new TimeoutException(waitingFor + " timed out after " + connectTimeout.toMillis() + "ms");
      timeout.initCause(e);
      throw timeout;
    }
  }

  private Result doRun() throws IOException {
    // Feature negotiation is never empty here (the endpoint always asks for some), so it's always sent.
    int helloOpaque = write(Opcode.HELLO, request -> {
      request.key(helloKey());
      KeyValueRequestBuilder.SectionWriter body = request.valueWriter();
      for (ServerFeature feature : requestedFeatures) {
        body.putShort(feature.value());
      }
    });
    startTiming(FeatureNegotiatingHandler.class);

    int errorMapOpaque = write(Opcode.ERROR_MAP, request -> request.valueWriter().putShort(ERROR_MAP_VERSION));
    startTiming(ErrorMapLoadingHandler.class);

    int listMechanismsOpaque = 0;
    if (credentials != null) {
      listMechanismsOpaque = write(Opcode.SASL_LIST_MECHS, request -> {
        // no key or value
      });
      startTiming(SaslListMechanismsHandler.class);
      startTiming(SaslAuthenticationHandler.class);
      startAuthentication(credentials.allowedMechanisms());
    } else {
      maybeSelectBucket();
    }
    io.flush();

    Set<ServerFeature> features = readHello(helloOpaque);
    ErrorMap errorMap = readErrorMap(errorMapOpaque);

    if (credentials != null) {
      readMechanisms(listMechanismsOpaque);
      authenticate();
    }

    if (bucketName.isPresent()) {
      if (!selectBucketSent) {
        maybeSelectBucket();
        io.flush();
      }
      readSelectBucket();
    }

    return new Result(features, errorMap);
  }

  // ---- HELLO (like FeatureNegotiatingHandler)

  private byte[] helloKey() {
    TreeMap<String, String> result = new TreeMap<>();

    String agent = endpointContext.environment().userAgent().formattedShort();
    if (agent == null || agent.isEmpty()) {
      agent = "couchbase-java-core/0.0.0";
    } else if (agent.length() > 200) {
      agent = agent.substring(0, 200);
    }
    result.put("a", agent);
    result.put("i", channelId);

    return Mapper.encodeAsBytes(result);
  }

  private Set<ServerFeature> readHello(int opaque) throws IOException {
    KeyValuePacket response = read(opaque);
    Duration latency = stopTiming(FeatureNegotiatingHandler.class);

    if (!successful(response)) {
      eventBus.publish(new FeaturesNegotiationFailedEvent(ioContext, response.status()));
      // Like FeatureNegotiatingHandler, which fails the connect attempt.
      throw new CouchbaseException(
        "Error while parsing negotiated server features.",
        new CouchbaseException(
          "HELO failed with status " + response.responseStatus() +
            " and body: " + response.bodyAsString()
        )
      );
    }

    Set<ServerFeature> negotiated = EnumSet.noneOf(ServerFeature.class);
    List<ServerFeature> unsolicited = new ArrayList<>();
    try {
      SectionReader body = response.body();
      while (body.remaining() > 0) {
        ServerFeature feature = ServerFeature.from(body.readUnsignedShort());
        if (requestedFeatures.contains(feature)) {
          negotiated.add(feature);
        } else {
          unsolicited.add(feature);
        }
      }
    } catch (Exception e) {
      throw new CouchbaseException("Error while parsing negotiated server features.", e);
    }

    if (!unsolicited.isEmpty()) {
      eventBus.publish(new UnsolicitedFeaturesReturnedEvent(ioContext, unsolicited));
    }
    eventBus.publish(new FeaturesNegotiatedEvent(ioContext, latency, new ArrayList<>(negotiated)));
    waitingFor = "KV Error Map loading";
    return negotiated;
  }

  // ---- GET_ERROR_MAP (like ErrorMapLoadingHandler)

  private @Nullable ErrorMap readErrorMap(int opaque) throws IOException {
    KeyValuePacket response = read(opaque);
    Duration latency = stopTiming(ErrorMapLoadingHandler.class);
    waitingFor = credentials != null ? "SASL Mechanism listing" : "KV Select Bucket loading";

    if (!successful(response)) {
      eventBus.publish(new ErrorMapLoadingFailedEvent(ioContext, latency, response.status()));
      return null;
    }

    Optional<ErrorMap> errorMap = parseErrorMap(response.bodyAsBytes());
    eventBus.publish(new ErrorMapLoadedEvent(ioContext, latency, errorMap));
    return errorMap.orElse(null);
  }

  private Optional<ErrorMap> parseErrorMap(byte[] input) {
    if (input.length == 0) {
      eventBus.publish(new ErrorMapUndecodableEvent(ioContext, "No content in response", ""));
      return Optional.empty();
    }
    try {
      return Optional.of(ErrorMap.fromJson(input));
    } catch (IOException e) {
      eventBus.publish(new ErrorMapUndecodableEvent(ioContext, e.getMessage(), new String(input, UTF_8)));
      return Optional.empty();
    }
  }

  // ---- SASL_LIST_MECHS (like SaslListMechanismsHandler)

  private void readMechanisms(int opaque) throws IOException {
    KeyValuePacket response = read(opaque);
    Duration latency = stopTiming(SaslListMechanismsHandler.class);
    waitingFor = "KV SASL Negotiation";

    if (!successful(response)) {
      throw failListingMechanisms("Received non-success status from server", response.status(), latency);
    }

    String[] rawMechanisms = response.bodyAsString().split(" ");
    boolean isEmpty = rawMechanisms.length == 1 && rawMechanisms[0].isEmpty();
    if (rawMechanisms.length == 0 || isEmpty) {
      throw failListingMechanisms("Received empty mechanism list from server", response.status(), latency);
    }

    serverMechanisms = Arrays.stream(rawMechanisms)
      .map(it -> {
        SaslMechanism mechanism = SaslMechanism.from(it);
        if (mechanism == null) {
          eventBus.publish(new UnknownSaslMechanismDetectedEvent(ioContext, it));
        }
        return mechanism;
      })
      .filter(Objects::nonNull)
      .collect(Collectors.toSet());
    eventBus.publish(new SaslMechanismsListedEvent(ioContext, serverMechanisms, latency));
  }

  private AuthenticationFailureException failListingMechanisms(String message, short status, Duration latency) {
    KeyValueIoErrorContext errorContext = new KeyValueIoErrorContext(
      MemcacheProtocol.decodeStatus(status), endpointContext, null
    );
    eventBus.publish(new SaslMechanismsListingFailedEvent(latency, errorContext, message));
    return new AuthenticationFailureException(message, errorContext, null);
  }

  // ---- SASL_AUTH and SASL_STEP (like SaslAuthenticationHandler)

  /**
   * Sends the first SASL request, using one of the given mechanisms (and SELECT_BUCKET too, if that's the only request).
   */
  private void startAuthentication(Set<SaslMechanism> mechanisms) throws IOException {
    SaslClient client;
    byte[] payload;
    try {
      client = createSaslClient(mechanisms);
      saslClient = client;

      SaslMechanism selectedMechanism = SaslMechanism.from(client.getMechanismName());
      roundtripsToGo = requireNonNull(selectedMechanism).roundtrips();
      eventBus.publish(new SaslMechanismsSelectedEvent(ioContext, mechanisms, selectedMechanism));

      payload = client.hasInitialResponse() ? client.evaluateChallenge(Bytes.EMPTY_BYTE_ARRAY) : null;
    } catch (SaslException e) {
      throw failAuthentication("SASL Client could not be constructed", null, e, (short) 0);
    }

    saslOpaque = writeSasl(Opcode.SASL_AUTH, client.getMechanismName(), payload);
    maybeSelectBucketAfterLastSaslRequest();
  }

  private SaslClient createSaslClient(Set<SaslMechanism> mechanisms) throws SaslException {
    SaslClient client = new CouchbaseSaslClientFactory().createSaslClient(
      mechanisms.stream().map(SaslMechanism::mech).toArray(String[]::new),
      null,
      "couchbase",
      ioContext.remoteSocket().toString(),
      null,
      this
    );
    if (client == null) {
      throw new SaslException("Failed to create SASL client for any of " + mechanisms);
    }
    return client;
  }

  /**
   * Reads SASL responses (sending SASL_STEP requests as needed) until authentication succeeds.
   */
  private void authenticate() throws IOException {
    while (true) {
      KeyValuePacket response = read(saslOpaque);
      roundtripsToGo--;
      short status = response.status();
      if (successful(response) || status == STATUS_AUTH_CONTINUE) {
        if (handleSaslResponse(response)) {
          return;
        }
      } else if (status == STATUS_AUTH_ERROR) {
        String message = "User authentication failed; check username and password.";
        if (!requireNonNull(credentials).allowedMechanisms().equals(setOf(SaslMechanism.PLAIN))) {
          message += " " + SaslAuthenticationHandler.LDAP_ADMONITION;
        }
        restartAuthenticationOrFail(message, response, status);
      } else {
        throw failAuthentication("Unexpected Status 0x" + Integer.toHexString(status) + " during SASL auth", response, null, status);
      }
    }
  }

  /**
   * @return true if authentication is complete, or false if another SASL request was sent.
   */
  private boolean handleSaslResponse(KeyValuePacket response) throws IOException {
    SaslClient client = requireNonNull(saslClient);
    try {
      byte opcode = response.opcode();
      if (opcode == Opcode.SASL_STEP.opcode() || client.isComplete()) {
        completeAuthentication(response);
        return true;
      }
      if (opcode != Opcode.SASL_AUTH.opcode()) {
        throw new CouchbaseException("Unexpected opcode 0x" + Integer.toHexString(opcode & 0xff) + " in SASL response");
      }

      byte[] evaluated;
      try {
        evaluated = client.evaluateChallenge(response.bodyAsBytes());
        if (evaluated == null || evaluated.length == 0) {
          throw new SaslException("Evaluation returned empty payload, this is unexpected!");
        }
      } catch (SaslException e) {
        throw failAuthentication("Failure while evaluating SASL Auth Response.", response, e, response.status());
      }

      saslOpaque = writeSasl(Opcode.SASL_STEP, client.getMechanismName(), evaluated);
      maybeSelectBucketAfterLastSaslRequest();
      io.flush();
      return false;

    } catch (AuthenticationFailureException e) {
      throw e;
    } catch (SaslException e) {
      throw failAuthentication("Unexpected error during SASL auth", response, e, response.status());
    } catch (IOException e) {
      throw e; // from writing the next request
    } catch (Exception e) {
      throw failAuthentication("Unexpected error during SASL auth", response, e, response.status());
    }
  }

  private void completeAuthentication(KeyValuePacket response) throws SaslException {
    SaslClient client = requireNonNull(saslClient);
    if (!client.isComplete()) {
      // validate final server response
      client.evaluateChallenge(response.bodyAsBytes());
      if (!client.isComplete()) {
        throw new SaslException("Incomplete SASL exchange");
      }
    }

    Duration latency = stopTiming(SaslAuthenticationHandler.class);
    endpointContext.authenticationStatus(AuthenticationStatus.SUCCEEDED);
    eventBus.publish(new SaslAuthenticationCompletedEvent(latency, ioContext));
    waitingFor = "KV Select Bucket loading";
  }

  /**
   * Like SaslAuthenticationHandler's maybeFailConnect: since the first SASL request is sent before the client knows
   * which mechanisms the server supports, authentication may have failed only because the client used a mechanism
   * the server doesn't support (for example, the server uses LDAP, which needs PLAIN). If so, tries again with
   * the mechanisms both support. Otherwise, fails.
   */
  private void restartAuthenticationOrFail(String message, KeyValuePacket response, short status) throws IOException {
    SaslMechanism currentMechanism = SaslMechanism.from(requireNonNull(saslClient).getMechanismName());
    if (serverMechanisms.contains(currentMechanism)) {
      throw failAuthentication(message, response, null, status);
    }

    Set<SaslMechanism> mergedMechanisms = requireNonNull(credentials).allowedMechanisms()
      .stream()
      .filter(serverMechanisms::contains)
      .collect(Collectors.toSet());
    if (mergedMechanisms.isEmpty()) {
      throw failAuthentication(
        "Could not negotiate SASL mechanism with server. " + SaslAuthenticationHandler.LDAP_ADMONITION,
        response, null, status
      );
    }

    eventBus.publish(new SaslAuthenticationRestartedEvent(ioContext));
    if (selectBucketSent) {
      // It was sent before authenticating, so it fails. Send it again after authenticating.
      discardedOpaques.add(selectBucketOpaque);
      selectBucketSent = false;
    }
    startAuthentication(mergedMechanisms);
    io.flush();
  }

  /**
   * Publishes the failure, and returns the exception to fail the connect attempt with.
   */
  private AuthenticationFailureException failAuthentication(
    String message,
    @Nullable KeyValuePacket lastPacket,
    @Nullable Throwable cause,
    short status
  ) {
    Duration latency = stopTiming(SaslAuthenticationHandler.class);

    // Unlike SaslAuthenticationHandler, never includes a copy of an invalid packet: packets are checked as they're read.
    byte[] packetCopy = Bytes.EMPTY_BYTE_ARRAY;
    Map<String, Object> serverContext = null;
    if (lastPacket != null) {
      byte[] content = lastPacket.bodyAsBytes();
      if (content.length != 0) {
        try {
          serverContext = Mapper.decodeInto(content, new TypeReference<Map<String, Object>>() {
          });
        } catch (Exception ex) {
          // Ignore, no displayable content
        }
      }
    }

    KeyValueIoErrorContext errorContext = new KeyValueIoErrorContext(
      MemcacheProtocol.decodeStatus(status), endpointContext, serverContext
    );
    endpointContext.authenticationStatus(AuthenticationStatus.FAILED);
    eventBus.publish(new SaslAuthenticationFailedEvent(latency, errorContext, message, packetCopy));
    return new AuthenticationFailureException(message, errorContext, cause);
  }

  /**
   * SASL callback: provides the username and password.
   */
  @Override
  public void handle(final Callback[] callbacks) throws UnsupportedCallbackException {
    SaslCredentials c = requireNonNull(credentials);
    for (Callback callback : callbacks) {
      if (callback instanceof NameCallback) {
        ((NameCallback) callback).setName(c.username());
      } else if (callback instanceof PasswordCallback) {
        ((PasswordCallback) callback).setPassword(c.password().toCharArray());
      } else {
        throw new UnsupportedCallbackException(callback, "Unexpected/Unsupported Callback");
      }
    }
  }

  // ---- SELECT_BUCKET (like SelectBucketHandler)

  /**
   * Like SaslAuthenticationHandler, which lets the next handler (selecting the bucket) send its request
   * along with the last SASL request.
   */
  private void maybeSelectBucketAfterLastSaslRequest() throws IOException {
    if (roundtripsToGo == 1) {
      maybeSelectBucket();
    }
  }

  private void maybeSelectBucket() throws IOException {
    if (!bucketName.isPresent() || selectBucketSent) {
      return;
    }
    byte[] bucket = bucketName.get().getBytes(UTF_8);
    selectBucketOpaque = write(Opcode.SELECT_BUCKET, request -> request.key(bucket));
    selectBucketSent = true;
    startTiming(SelectBucketHandler.class);
  }

  private void readSelectBucket() throws IOException {
    String bucket = bucketName.orElseThrow(IllegalStateException::new);
    KeyValuePacket response = read(selectBucketOpaque);
    Duration latency = stopTiming(SelectBucketHandler.class);

    short status = response.status();
    if (status == MemcacheProtocol.Status.SUCCESS.status()) {
      eventBus.publish(new SelectBucketCompletedEvent(latency, ioContext, bucket));

    } else if (status == MemcacheProtocol.Status.ACCESS_ERROR.status()) {
      eventBus.publish(new SelectBucketFailedEvent(Event.Severity.WARN, ioContext, status));
      // We don't set AuthenticationStatus.FAILED here, as it's not unambiguously that.
      throw new AuthenticationFailureException(
        "Either the bucket with name \"" + redactMeta(bucket) + "\" is not present, the user does not have " +
          "the right privileges to access it, or the bucket is hibernated",
        new KeyValueIoErrorContext(MemcacheProtocol.decodeStatus(status), endpointContext, null),
        null
      );

    } else if (status == MemcacheProtocol.Status.NOT_FOUND.status()) {
      // NOT_FOUND severity is lowered to debug because it shows up when bootstrapping against
      // a node in a MDS setup without the kv service / bucket enabled.
      eventBus.publish(new SelectBucketFailedEvent(Event.Severity.DEBUG, ioContext, status));
      throw BucketNotFoundException.forBucket(bucket);

    } else {
      eventBus.publish(new SelectBucketFailedEvent(Event.Severity.ERROR, ioContext, status));
      throw new CouchbaseException("Select bucket failed with unexpected status code 0x" + Integer.toHexString(status));
    }
  }

  // ---- helpers

  /**
   * Buffers a request (see {@link PacketIo#write}), and returns its opaque.
   */
  private int write(Opcode opcode, Consumer<KeyValueRequestBuilder> build) throws IOException {
    int opaque = BaseKeyValueRequest.nextOpaque();
    io.write(opcode, opaque, build);
    return opaque;
  }

  /**
   * Buffers a SASL request, and returns its opaque.
   *
   * @param payload null for none.
   */
  private int writeSasl(Opcode opcode, String mechanism, byte @Nullable [] payload) throws IOException {
    byte[] key = mechanism.getBytes(UTF_8);
    byte[] value = payload == null ? Bytes.EMPTY_BYTE_ARRAY : payload;
    return write(opcode, request -> request.key(key).value(value));
  }

  private static boolean successful(KeyValuePacket response) {
    return response.status() == MemcacheProtocol.Status.SUCCESS.status();
  }

  /**
   * Returns the response with the given opaque, handling any requests the server pushes, and skipping
   * the responses to discarded requests.
   */
  private KeyValuePacket read(int opaque) throws IOException {
    while (true) {
      KeyValuePacket packet = io.read();

      if (ServerPushHandler.isServerPush(packet)) {
        ServerPushHandler.handleServerPush(endpointContext, packet);
        continue;
      }

      int actual = packet.opaque();
      if (actual == opaque) {
        return packet;
      }

      if (!discardedOpaques.remove(actual)) {
        // The server answers handshake requests in order, so this is a protocol error.
        throw new CouchbaseException("Unexpected response during KV connection handshake (expected opaque "
          + opaque + ", got " + actual + "): " + packet);
      }
    }
  }

  private void startTiming(Class<?> step) {
    timingStarts.put(step.getSimpleName(), System.nanoTime());
  }

  private Duration stopTiming(Class<?> step) {
    Long start = timingStarts.remove(step.getSimpleName());
    if (start == null) {
      return Duration.ZERO;
    }
    Duration latency = Duration.ofNanos(System.nanoTime() - start);
    timings.put(step.getSimpleName(), latency);
    return latency;
  }
}
