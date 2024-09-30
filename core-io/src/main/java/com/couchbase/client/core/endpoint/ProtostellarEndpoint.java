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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.CoreCouchbaseOps;
import com.couchbase.client.core.cnc.AbstractContext;
import com.couchbase.client.core.cnc.Context;
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
import com.couchbase.client.core.deps.io.grpc.ManagedChannel;
import com.couchbase.client.core.deps.io.grpc.ManagedChannelBuilder;
import com.couchbase.client.core.deps.io.grpc.Metadata;
import com.couchbase.client.core.deps.io.grpc.MethodDescriptor;
import com.couchbase.client.core.deps.io.grpc.Status;
import com.couchbase.client.core.deps.io.grpc.netty.GrpcSslContexts;
import com.couchbase.client.core.deps.io.grpc.netty.NettyChannelBuilder;
import com.couchbase.client.core.deps.io.netty.channel.ChannelOption;
import com.couchbase.client.core.diagnostics.AuthenticationStatus;
import com.couchbase.client.core.deps.io.netty.handler.ssl.SslContext;
import com.couchbase.client.core.diagnostics.ClusterState;
import com.couchbase.client.core.diagnostics.EndpointDiagnostics;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.SecurityConfig;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.FeatureNotAvailableException;
import com.couchbase.client.core.error.SecurityException;
import com.couchbase.client.core.error.UnambiguousTimeoutException;
import com.couchbase.client.core.error.context.CancellationErrorContext;
import com.couchbase.client.core.protostellar.GrpcAwareRequestTracer;
import com.couchbase.client.core.protostellar.ProtostellarContext;
import com.couchbase.client.core.protostellar.ProtostellarStatsCollector;
import com.couchbase.client.core.util.Deadline;
import com.couchbase.client.core.util.HostAndPort;
import com.couchbase.client.protostellar.admin.bucket.v1.BucketAdminServiceGrpc;
import com.couchbase.client.protostellar.admin.collection.v1.CollectionAdminServiceGrpc;
import com.couchbase.client.protostellar.admin.search.v1.SearchAdminServiceGrpc;
import com.couchbase.client.protostellar.analytics.v1.AnalyticsServiceGrpc;
import com.couchbase.client.protostellar.internal.hooks.v1.HooksServiceGrpc;
import com.couchbase.client.protostellar.kv.v1.KvServiceGrpc;
import com.couchbase.client.protostellar.query.v1.QueryServiceGrpc;
import com.couchbase.client.protostellar.search.v1.SearchServiceGrpc;

import javax.net.ssl.SSLException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;

/**
 * Wraps a GRPC ManagedChannel.
 */
public class ProtostellarEndpoint {

  // JVMCBC-1187: Temporary performance-related code that will be removed pre-GA.  (It's useful, but GRPC doesn't expose internals well, and
  // this has to use hacky methods to get them). Plus of course it's a hack to have a public static setter.
  public static ProtostellarStatsCollector collector;

  private final AtomicBoolean shutdown = new AtomicBoolean(false);
  private final ManagedChannel managedChannel;
  private final KvServiceGrpc.KvServiceFutureStub kvStub;
  private final KvServiceGrpc.KvServiceBlockingStub kvBlockingStub;
  private final AnalyticsServiceGrpc.AnalyticsServiceStub analyticsStub;
  private final QueryServiceGrpc.QueryServiceStub queryStub;
  private final SearchServiceGrpc.SearchServiceStub searchStub;
  private final HooksServiceGrpc.HooksServiceBlockingStub hooksBlockingStub;
  private final CollectionAdminServiceGrpc.CollectionAdminServiceFutureStub collectionAdminStub;
  private final BucketAdminServiceGrpc.BucketAdminServiceFutureStub bucketAdminStub;
  private final SearchAdminServiceGrpc.SearchAdminServiceFutureStub searchAdminStub;
  private final HostAndPort remote;
  private final CoreEnvironment env;
  private final ProtostellarContext ctx;

  public ProtostellarEndpoint(ProtostellarContext ctx, HostAndPort remote) {
    // JVMCBC-1187: This is a temporary solution to get performance testing working, which will be removed pre-GA.
    // The issue is that we send in protostellar://cbs as a connection string because the Couchbase cluster is running in a Docker container named "cbs", and the performer is also running in a Docker container.
    // However, Stellar Nebula is running normally, not in a container, so must be accessed with "localhost" instead.
    // So we pass a connection string of "protostellar://cbs" and "com.couchbase.protostellar.overrideHostname"="localhost".
    String override = System.getProperty("com.couchbase.protostellar.overrideHostname");
    this.remote = override != null
      ? new HostAndPort(override, remote.port())
      : remote;

    this.ctx = requireNonNull(ctx);
    this.env = ctx.environment();
    this.managedChannel = channel(ctx);


    // This getState is inherently non-atomic.  However, nothing should be able to use this channel or endpoint yet, so it should be guaranteed to be IDLE.
    ConnectivityState now = this.managedChannel.getState(false);
    notifyOnChannelStateChange(now);

    CallCredentials creds = ctx.authenticator().protostellarCallCredentials();

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
    kvStub = KvServiceGrpc.newFutureStub(managedChannel).withInterceptors(ci).withCallCredentials(creds);
    kvBlockingStub = KvServiceGrpc.newBlockingStub(managedChannel).withInterceptors(ci).withCallCredentials(creds);
    analyticsStub = AnalyticsServiceGrpc.newStub(managedChannel).withCallCredentials(creds);
    queryStub = QueryServiceGrpc.newStub(managedChannel).withCallCredentials(creds);
    searchStub = SearchServiceGrpc.newStub(managedChannel).withCallCredentials(creds);
    hooksBlockingStub = HooksServiceGrpc.newBlockingStub(managedChannel).withCallCredentials(creds);
    collectionAdminStub = CollectionAdminServiceGrpc.newFutureStub(managedChannel).withCallCredentials(creds);
    bucketAdminStub = BucketAdminServiceGrpc.newFutureStub(managedChannel).withCallCredentials(creds);
    searchAdminStub = SearchAdminServiceGrpc.newFutureStub(managedChannel).withCallCredentials(creds);
  }

  private ManagedChannel channel(ProtostellarContext ctx) {
    SecurityConfig securityConfig = ctx.environment().securityConfig();

    SslContext sslContext;

    try {
      if (securityConfig.trustManagerFactory() != null) {
        sslContext = GrpcSslContexts.forClient()
          .trustManager(securityConfig.trustManagerFactory())
          .build();
      } else if (!securityConfig.trustCertificates().isEmpty()) {
        sslContext = GrpcSslContexts.forClient()
          .trustManager(securityConfig.trustCertificates())
          .build();
      } else {
        throw new CouchbaseException("Internal bug - should not reach here");
      }
    } catch (SSLException e) {
      throw new SecurityException(e);
    }

    ManagedChannelBuilder builder = NettyChannelBuilder.forAddress(remote.host(), remote.port())
      .sslContext(sslContext)

      // 20MB is the (current) maximum document size supported by the server.  Specifying 21MB to give wiggle room for the rest of the GRPC message.
      .maxInboundMessageSize(21 * 1024 * 1024) // Max Couchbase document size (20 MiB) plus some slack
      .executor(env.executor())
      .withOption(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) env.timeoutConfig().connectTimeout().toMillis())
      // Retry strategies to be determined, but presumably we will need something custom rather than what GRPC provides
      .disableRetry();

    if (ctx.coreResources().requestTracer() != null
      && ctx.coreResources().requestTracer() instanceof GrpcAwareRequestTracer) {
      ((GrpcAwareRequestTracer) ctx.coreResources().requestTracer()).registerGrpc(builder);
    }

    // JVMCBC-1187: experimental code for performance testing that will be removed pre-GA.
    // Testing anyway indicates this load balancing makes zero difference - always end up with one channel and one subchannel per ManagedChannel regardless.
    String loadBalancingCount = System.getProperty("com.couchbase.protostellar.loadBalancing");
    String loadBalancingStrategy = System.getProperty("com.couchbase.protostellar.loadBalancingStrategy", "round_robin");
    String loadBalancingSingle = System.getProperty("com.couchbase.protostellar.loadBalancingSingle", "true");

    if (loadBalancingCount != null) {
      List<EquivalentAddressGroup> addresses = new ArrayList<>();

      int count = Integer.parseInt(loadBalancingCount);
      boolean single = Boolean.parseBoolean(loadBalancingSingle);

      if (single) {
        List<SocketAddress> adds = new ArrayList<>();
        for (int i = 0; i < count; i ++) {
          adds.add(newInetSocketAddress(remote));
        }
        addresses.add(new EquivalentAddressGroup(adds));
      }
      else {
        for (int i = 0; i < count; i ++) {
          addresses.add(new EquivalentAddressGroup(newInetSocketAddress(remote)));
        }
      }

      MultiAddressNameResolverFactory nameResolverFactory = new MultiAddressNameResolverFactory(addresses);

      // Deprecated API but the replacement is unclear and this code may only be used during development.
      builder.nameResolverFactory(nameResolverFactory)
        .defaultLoadBalancingPolicy(loadBalancingStrategy);
    }

    return builder.build();
  }

  private static InetSocketAddress newInetSocketAddress(HostAndPort hostAndPort) {
    return new InetSocketAddress(hostAndPort.host(), hostAndPort.port());
  }

  private void notifyOnChannelStateChange(ConnectivityState current) {
    this.managedChannel.notifyWhenStateChanged(current, () -> {
      ConnectivityState now = this.managedChannel.getState(false);
      Context ec = new ProtostellarEndpointContext(ctx, remote);
      env.eventBus().publish(new EndpointStateChangedEvent(ec, convert(current), convert(now)));

      notifyOnChannelStateChange(now);
    });
  }

  private static class ProtostellarEndpointContext extends AbstractContext {
    private final ProtostellarContext ctx;
    private final HostAndPort remote;

    public ProtostellarEndpointContext(ProtostellarContext ctx, HostAndPort remote) {
      this.ctx = requireNonNull(ctx);
      this.remote = requireNonNull(remote);
    }

    @Override
    public void injectExportableParams(final Map<String, Object> input) {
      ctx.injectExportableParams(input);
      input.put("remote", remote.toString());
    }
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
      remote.format(),
      Optional.empty(),
      Optional.empty(),
      Optional.empty(),
      Optional.empty()
      );
  }

  public synchronized void shutdown(Duration timeout) {
    if (shutdown.compareAndSet(false, true)) {
      managedChannel.shutdown();
      try {
        managedChannel.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
      }
    }
  }

  public KvServiceGrpc.KvServiceFutureStub kvStub() {
    return kvStub;
  }

  public KvServiceGrpc.KvServiceBlockingStub kvBlockingStub() {
    return kvBlockingStub;
  }

  public AnalyticsServiceGrpc.AnalyticsServiceStub analyticsStub() {
    return analyticsStub;
  }

  public QueryServiceGrpc.QueryServiceStub queryStub() {
    return queryStub;
  }

  public SearchServiceGrpc.SearchServiceStub searchStub() {
    return searchStub;
  }

  public HooksServiceGrpc.HooksServiceBlockingStub hooksBlockingStub() {
    return hooksBlockingStub;
  }

  public CollectionAdminServiceGrpc.CollectionAdminServiceFutureStub collectionAdminStub() {
    return collectionAdminStub;
  }

  public BucketAdminServiceGrpc.BucketAdminServiceFutureStub bucketAdminStub() {
    return bucketAdminStub;
  }

  public SearchAdminServiceGrpc.SearchAdminServiceFutureStub searchAdminStub() {
    return searchAdminStub;
  }

  /**
   * Note that this is synchronized against something that could block for some time - but only during shutdown.
   * <p>
   * It's synchronized to make the shutdown process atomic.
   */
  public synchronized boolean isShutdown() {
    return shutdown.get();
  }

  public HostAndPort hostAndPort() {
    return remote;
  }

  /**
   * Waits until the ManagedChannel is in READY state.  Will also initialise trying to make that connection if it's not already.
   *
   * @return a CompletableFuture, as that's what {@link CoreCouchbaseOps#waitUntilReady(Set, Duration, ClusterState, String)}  uses.
   */
  @Stability.Internal
  public CompletableFuture<Void> waitUntilReady(Deadline deadline, boolean waitingForReady) {
    CompletableFuture<Void> onDone = new CompletableFuture<>();
    ConnectivityState current = managedChannel.getState(true);
    notify(current, onDone, deadline, waitingForReady);
    return onDone;
  }

  private void notify(ConnectivityState current, CompletableFuture<Void> onDone, Deadline deadline, boolean waitingForReady) {
    if (inDesiredState(current, waitingForReady)) {
      onDone.complete(null);
    }
    else {
      ctx.environment().timer().schedule(() -> {
        ConnectivityState now = this.managedChannel.getState(true);

        if (inDesiredState(current, waitingForReady)) {
          onDone.complete(null);
        } else if (deadline.exceeded()) {
          onDone.completeExceptionally(new UnambiguousTimeoutException("Timed out while waiting for Protostellar endpoint " + remote, new CancellationErrorContext(null)));
        } else {
          notify(now, onDone, deadline, waitingForReady);
        }
      }, Duration.ofMillis(50));
    }
  }

  private boolean inDesiredState(ConnectivityState current, boolean waitingForReady) {
    return (waitingForReady && current == ConnectivityState.READY) || (!waitingForReady && current != ConnectivityState.READY);
  }
}
