/*
 * Copyright (c) 2023 Couchbase, Inc.
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

package com.couchbase.client.core.endpoint;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.events.endpoint.EndpointStateChangedEvent;
import com.couchbase.client.core.deps.io.grpc.Attributes;
import com.couchbase.client.core.deps.io.grpc.CallCredentials;
import com.couchbase.client.core.deps.io.grpc.CallOptions;
import com.couchbase.client.core.deps.io.grpc.Channel;
import com.couchbase.client.core.deps.io.grpc.ClientCall;
import com.couchbase.client.core.deps.io.grpc.ClientInterceptor;
import com.couchbase.client.core.deps.io.grpc.ClientStreamTracer;
import com.couchbase.client.core.deps.io.grpc.ConnectivityState;
import com.couchbase.client.core.deps.io.grpc.EquivalentAddressGroup;
import com.couchbase.client.core.deps.io.grpc.InsecureChannelCredentials;
import com.couchbase.client.core.deps.io.grpc.ManagedChannel;
import com.couchbase.client.core.deps.io.grpc.ManagedChannelBuilder;
import com.couchbase.client.core.deps.io.grpc.Metadata;
import com.couchbase.client.core.deps.io.grpc.MethodDescriptor;
import com.couchbase.client.core.deps.io.grpc.Status;
import com.couchbase.client.core.deps.io.grpc.netty.NettyChannelBuilder;
import com.couchbase.client.core.deps.io.netty.channel.ChannelOption;
import com.couchbase.client.core.diagnostics.EndpointDiagnostics;
import com.couchbase.client.core.error.UnambiguousTimeoutException;
import com.couchbase.client.core.error.context.CancellationErrorContext;
import com.couchbase.client.core.protostellar.ProtostellarStatsCollector;
import com.couchbase.client.core.util.HostAndPort;
import com.couchbase.client.protostellar.analytics.v1.AnalyticsGrpc;
import com.couchbase.client.protostellar.internal.hooks.v1.HooksGrpc;
import com.couchbase.client.protostellar.kv.v1.KvGrpc;
import com.couchbase.client.protostellar.query.v1.QueryGrpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Wraps a GRPC ManagedChannel.
 */
public class ProtostellarEndpoint {
  private final Logger logger = LoggerFactory.getLogger(ProtostellarEndpoint.class);

  // JVMCBC-1187: Temporary performance-related code that will be removed pre-GA.  (It's useful, but GRPC doesn't expose internals well, and
  // this has to use hacky methods to get them). Plus of course it's a hack to have a public static setter.
  public static ProtostellarStatsCollector collector;

  private final AtomicBoolean shutdown = new AtomicBoolean(false);
  private final ManagedChannel managedChannel;
  private final KvGrpc.KvFutureStub kvStub;
  private final KvGrpc.KvBlockingStub kvBlockingStub;
  private final AnalyticsGrpc.AnalyticsStub analyticsStub;
  private final QueryGrpc.QueryStub queryStub;
  private final HooksGrpc.HooksBlockingStub hooksBlockingStub;
  private final String hostname;
  private final int port;
  private final Core core;

  public ProtostellarEndpoint(Core core, String hostname, final int port) {
    // JVMCBC-1187: This is a temporary solution to get performance testing working, which will be removed pre-GA.
    // The issue is that we send in protostellar://cbs as a connection string because the Couchbase cluster is running in a Docker container named "cbs", and the performer is also running in a Docker container.
    // However, Stellar Nebula is running normally, not in a container, so must be accessed with "localhost" instead.
    // So we pass a connection string of "protostellar://cbs" and "com.couchbase.protostellar.overrideHostname"="localhost".
    String override = System.getProperty("com.couchbase.protostellar.overrideHostname");
    // JVMCBC-1187: all Protostellar logging will be tidied up or removed pre-GA.
    logger.info("creating {} {}, override={}", hostname, port, override);
    if (override != null) {
      hostname = override;
    }
    this.hostname = hostname;
    this.port = port;
    this.core = core;
    this.managedChannel = channel();


    // This getState is inherently non-atomic.  However, nothing should be able to use this channel or endpoint yet, so it should be guaranteed to be IDLE.
    ConnectivityState now = this.managedChannel.getState(false);
    logger.info("channel starts in state {}/{}", now, convert(now));
    notifyOnChannelStateChange(now);

    CallCredentials creds = new CallCredentials() {
      @Override
      public void applyRequestMetadata(RequestInfo requestInfo, Executor executor, MetadataApplier applier) {
        executor.execute(() -> {
          try {
            Metadata headers = new Metadata();
            core.context().authenticator().authProtostellarRequest(headers);
            applier.apply(headers);
          } catch (Throwable e) {
            applier.fail(Status.UNAUTHENTICATED.withCause(e));
          }
        });
      }

      @Override
      public void thisUsesUnstableApi() {
      }
    };

    // JVMCBC-1187: Temporary code to provide some insight on GRPC internals, will likely be removed pre-GA.
    ClientStreamTracer.Factory factory = new ClientStreamTracer.Factory() {
      public ClientStreamTracer newClientStreamTracer(ClientStreamTracer.StreamInfo info, Metadata headers) {
        return new ClientStreamTracer() {
          @Override
          public void outboundMessageSent(int seqNo, long optionalWireSize, long optionalUncompressedSize) {
            super.outboundMessageSent(seqNo, optionalWireSize, optionalUncompressedSize);
            if (collector != null) {
              collector.outboundMessageSent();
            }
          }

          @Override
          public void outboundMessage(int seqNo) {
            super.outboundMessage(seqNo);
            // not very useful, seqno always 0
            // logger.info("outbound {}", seqNo);

            // opsActuallyInFlight only reaches the max threads of the underlying executor.  This _might_ be the rpcs that are being concurrently
            // sent on the wire, rather than RPCs that are in-flight.
            if (collector != null) {
              collector.outboundMessage();
            }
          }

          @Override
          public void inboundMessage(int seqNo) {
            super.inboundMessage(seqNo);
            if (collector != null) {
              collector.inboundMessage();
            }
          }

          @Override
          public void inboundMessageRead(int seqNo, long optionalWireSize, long optionalUncompressedSize) {
            super.inboundMessageRead(seqNo, optionalWireSize, optionalUncompressedSize);
            if (collector != null) {
              collector.inboundMessageRead();
            }

          }

          @Override
          public void streamCreated(Attributes transportAttrs, Metadata headers) {
            super.streamCreated(transportAttrs, headers);
            // not very useful, doesn't give the stream id - many streams created and destroyed constantly
            // logger.info("stream created");
          }

          @Override
          public void streamClosed(Status status) {
            super.streamClosed(status);
            // logger.info("stream closed");
          }
        };
      }
    };

    ClientInterceptor ci = new ClientInterceptor() {
      @Override
      public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
        // logger.info("{}", method.getFullMethodName());

        // JVMCBC-1187: an interceptor seems to be the only way of setting this option.  Will be removed before GA.
        return next.newCall(method, callOptions.withStreamTracerFactory(factory));
      }
    };

    // withWaitForReady exists but better to do retries ourselves for ErrorContext transparency.
    kvStub = KvGrpc.newFutureStub(managedChannel).withInterceptors(ci);
    kvBlockingStub = KvGrpc.newBlockingStub(managedChannel).withInterceptors(ci);
    analyticsStub = AnalyticsGrpc.newStub(managedChannel).withCallCredentials(creds);
    queryStub = QueryGrpc.newStub(managedChannel).withCallCredentials(creds);
    hooksBlockingStub = HooksGrpc.newBlockingStub(managedChannel).withCallCredentials(creds);
  }

  private ManagedChannel channel() {
    logger.info("making channel {} {}", hostname, port);

    // JVMCBC-1187: we're using unverified TLS for now - once STG has it we can use the same Capella cert bundling approach and use TLS properly.
    ManagedChannelBuilder builder = NettyChannelBuilder.forAddress(hostname, port, InsecureChannelCredentials.create())
      // 20MB is the (current) maximum document size supported by the server.  Specifying 21MB to give wiggle room for the rest of the GRPC message.
      .maxInboundMessageSize(22020096)
      .executor(core.context().environment().executor())
      .withOption(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) core.context().environment().timeoutConfig().connectTimeout().toMillis())
      // Retry strategies to be determined, but presumably we will need something custom rather than what GRPC provides
      .disableRetry();

    // JVMCBC-1187: experimental code for performance testing that will be removed pre-GA.
    // Testing anyway indicates this load balancing makes zero difference - always end up with one channel and one subchannel per ManagedChannel regardless.
    String loadBalancingCount = System.getProperty("com.couchbase.protostellar.loadBalancing");
    String loadBalancingStrategy = System.getProperty("com.couchbase.protostellar.loadBalancingStrategy", "round_robin");
    String loadBalancingSingle = System.getProperty("com.couchbase.protostellar.loadBalancingSingle", "true");
    logger.info("loadBalancing={} loadBalancingStrategy={} loadBalancingSingle={}", loadBalancingCount, loadBalancingStrategy, loadBalancingSingle);

    if (loadBalancingCount != null) {
      List<EquivalentAddressGroup> addresses = new ArrayList<>();

      int count = Integer.parseInt(loadBalancingCount);
      boolean single = Boolean.parseBoolean(loadBalancingSingle);

      if (single) {
        List<SocketAddress> adds = new ArrayList<>();
        for (int i = 0; i < count; i ++) {
          adds.add(new InetSocketAddress(hostname, port));
        }
        addresses.add(new EquivalentAddressGroup(adds));
      }
      else {
        for (int i = 0; i < count; i ++) {
          addresses.add(new EquivalentAddressGroup(new InetSocketAddress(hostname, port)));
        }
      }

      MultiAddressNameResolverFactory nameResolverFactory = new MultiAddressNameResolverFactory(addresses);

      // Deprecated API but the replacement is unclear and this code may only be used during development.
      builder.nameResolverFactory(nameResolverFactory)
        .defaultLoadBalancingPolicy(loadBalancingStrategy);
    }

    return builder.build();
  }

  private void notifyOnChannelStateChange(ConnectivityState current) {
    this.managedChannel.notifyWhenStateChanged(current, () -> {
      ConnectivityState now = this.managedChannel.getState(false);
      logger.info("channel has changed state from {}/{} to {}/{}", current, convert(current), now, convert(now));

      EndpointContext ec = new EndpointContext(core.context(),
        new HostAndPort(hostname, port),
        null,
        null,
        Optional.empty(),
        Optional.empty(),
        Optional.empty());

      core.context().environment().eventBus().publish(new EndpointStateChangedEvent(ec, convert(current), convert(now)));

      notifyOnChannelStateChange(now);
    });
  }

  private static EndpointState convert(ConnectivityState state) {
    switch (state) {
      case IDLE:
        // Channels that haven't had any RPCs yet or in a while will be in this state.
        return EndpointState.DISCONNECTED;
      case READY:
        return EndpointState.CONNECTED;
      case SHUTDOWN:
        return EndpointState.DISCONNECTING;
      case TRANSIENT_FAILURE:
      case CONNECTING:
        return EndpointState.CONNECTING;
    }
    throw new IllegalStateException("Unknown state " + state);
  }

  // Diagnostics to be completed under JVMCBC-1189
  public EndpointDiagnostics diagnostics() {
    return new EndpointDiagnostics(null,
      convert(managedChannel.getState(false)),
      CircuitBreaker.State.CLOSED,
      // Don't have easy access to the local hostname - try to resolve under JVMCBC-1189
      null,
      hostname,
      Optional.empty(),
      Optional.empty(),
      Optional.empty(),
      Optional.empty());
  }

  public synchronized void shutdown(Duration timeout) {
    if (shutdown.compareAndSet(false, true)) {
      logger.info("waiting for channel to shutdown");
      managedChannel.shutdown();
      try {
        managedChannel.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
      }
      logger.info("channel has shutdown");
    }
  }

  public KvGrpc.KvFutureStub kvStub() {
    return kvStub;
  }

  public KvGrpc.KvBlockingStub kvBlockingStub() {
    return kvBlockingStub;
  }

  public AnalyticsGrpc.AnalyticsStub analyticsStub() {
    return analyticsStub;
  }

  public QueryGrpc.QueryStub queryStub() {
    return queryStub;
  }

  public HooksGrpc.HooksBlockingStub hooksBlockingStub() {
    return hooksBlockingStub;
  }

  /**
   * Note that this is synchronized against something that could block for some time - but only during shutdown.
   * <p>
   * It's synchronized to make the shutdown process atomic.
   */
  public synchronized boolean isShutdown() {
    return shutdown.get();
  }

  public String hostname() {
    return hostname;
  }

  public int port() {
    return port;
  }

  /**
   * Waits until the ManagedChannel is in READY state.  Will also initialise trying to make that connection if it's not already.
   *
   * @return a CompletableFuture as that's what WaitUntilReadyHelper uses.
   */
  @Stability.Internal
  public CompletableFuture<Void> waitUntilReady(long absoluteTimeoutNanos, boolean waitingForReady) {
    CompletableFuture<Void> onDone = new CompletableFuture<>();
    ConnectivityState current = managedChannel.getState(true);
    logger.debug("WaitUntilReady: Endpoint {}:{} starts in state {}", hostname, port, current);
    notify(current, onDone, absoluteTimeoutNanos, waitingForReady);
    return onDone;
  }

  private void notify(ConnectivityState current, CompletableFuture<Void> onDone, long absoluteTimeoutNanos, boolean waitingForReady) {
    if (inDesiredState(current, waitingForReady)) {
      onDone.complete(null);
    }
    else {
      this.managedChannel.notifyWhenStateChanged(current, () -> {
        ConnectivityState now = this.managedChannel.getState(true);
        logger.debug("WaitUntilReady: Endpoint {}:{} is now in state {}", hostname, port, now);

        if (inDesiredState(current, waitingForReady)) {
          onDone.complete(null);
        } else if (System.nanoTime() >= absoluteTimeoutNanos) {
          onDone.completeExceptionally(new UnambiguousTimeoutException("Timed out while waiting for Protostellar endpoint " + hostname + ":" + port, new CancellationErrorContext(null)));
        } else {
          notify(now, onDone, absoluteTimeoutNanos, waitingForReady);
        }
      });
    }
  }

  private boolean inDesiredState(ConnectivityState current, boolean waitingForReady) {
    return (waitingForReady && current == ConnectivityState.READY) || (!waitingForReady && current != ConnectivityState.READY);
  }
}
