/*
 * Copyright 2023 Couchbase, Inc.
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

package com.couchbase.client.core.io.netty.kv;

import com.couchbase.client.core.cnc.Event;
import com.couchbase.client.core.cnc.events.endpoint.EndpointWriteFailedEvent;
import com.couchbase.client.core.config.ConfigVersion;
import com.couchbase.client.core.config.ConfigurationProvider;
import com.couchbase.client.core.config.ProposedBucketConfigContext;
import com.couchbase.client.core.config.ProposedGlobalConfigContext;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.channel.Channel;
import com.couchbase.client.core.deps.io.netty.channel.ChannelFuture;
import com.couchbase.client.core.endpoint.EndpointContext;
import com.couchbase.client.core.json.Mapper;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.IntSupplier;

import static com.couchbase.client.core.deps.io.netty.buffer.Unpooled.EMPTY_BUFFER;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noBody;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noCas;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noDatatype;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noExtras;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noKey;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noPartition;
import static java.util.Objects.requireNonNull;

/**
 * Not thread-safe. All methods must be called from the Netty event loop.
 */
public class ClustermapChangeNotificationHandler {
  private Integer outstandingRequestOpaque;

  private ConfigVersion notifiedVersion = ConfigVersion.ZERO;

  private final IntSupplier opaqueGenerator;
  private final EndpointContext endpointContext;
  private final Channel channel;
  private final ConfigurationProvider configurationProvider;
  private final String origin; // hostname or IP address

  public ClustermapChangeNotificationHandler(
    EndpointContext endpointContext,
    IntSupplier opaqueGenerator,
    Channel channel,
    String origin
  ) {
    this.opaqueGenerator = requireNonNull(opaqueGenerator);
    this.endpointContext = requireNonNull(endpointContext);
    this.channel = requireNonNull(channel);
    this.configurationProvider = requireNonNull(endpointContext.core().configurationProvider());
    this.origin = requireNonNull(origin);
  }

  public void getBaselineConfig() {
    onConfigChangeNotification(ConfigVersion.ZERO);
  }

  public void onConfigChangeNotification(ConfigVersion version) {
    assertEventLoop();

    this.notifiedVersion = version;

    if (outstandingRequestOpaque != null) {
      // Maybe send another request after the outstanding request completes.
      return;
    }

    sendGetConfigRequest();
  }

  public boolean maybeHandleConfigResponse(ByteBuf message) {
    assertEventLoop();

    if (!Objects.equals(outstandingRequestOpaque, MemcacheProtocol.opaque(message))) {
      return false; // not ours!
    }

    outstandingRequestOpaque = null;

    String config = MemcacheProtocol.bodyAsString(message);
    String bucketName = MemcacheProtocol.key(message).orElse(EMPTY_BUFFER).toString(StandardCharsets.UTF_8);

    if (!bucketName.isEmpty()) {
      configurationProvider.proposeBucketConfig(new ProposedBucketConfigContext(bucketName, config, origin));
    } else {
      configurationProvider.proposeGlobalConfig(new ProposedGlobalConfigContext(config, origin));
    }

    ObjectNode configNode = (ObjectNode) Mapper.decodeIntoTree(config);
    ConfigVersion version = parseConfigVersion(configNode);
    if (!notifiedVersion.isLessThanOrEqualTo(version)) {
      sendGetConfigRequest();
    }

    return true;
  }

  private void assertEventLoop() {
    if (!channel.eventLoop().inEventLoop()) {
      throw new IllegalStateException("This method must only be called from the channel's event loop.");
    }
  }

  private void sendGetConfigRequest() {
    outstandingRequestOpaque = opaqueGenerator.getAsInt();
    ByteBuf request = MemcacheProtocol.request(channel.alloc(), MemcacheProtocol.Opcode.GET_CONFIG, noDatatype(),
      noPartition(), outstandingRequestOpaque, noCas(), noExtras(), noKey(), noBody());

    try {
      channel.writeAndFlush(request).addListener((ChannelFuture writeFuture) -> {
        if (writeFuture.isSuccess()) {
          return;
        }

        boolean active = channel.isActive();

        Event.Severity severity = !active ? Event.Severity.DEBUG : Event.Severity.WARN;
        endpointContext.environment().eventBus()
          .publish(new EndpointWriteFailedEvent(severity, endpointContext, writeFuture.cause()));

        if (active) {
          long retryDelayMillis = 10 + (int) (250 * Math.random());
          channel.eventLoop()
            .schedule(this::sendGetConfigRequest, retryDelayMillis, TimeUnit.MILLISECONDS);
        }
      });

    } catch (Throwable t) {
      request.release();
      channel.close();
    }
  }

  private static ConfigVersion parseConfigVersion(ObjectNode json) {
    // absent "revEpoch" field means server is too old to know about epochs, so... zero
    long epoch = json.path("revEpoch").longValue(); // zero if absent
    JsonNode revNode = json.path("rev");
    if (!revNode.isIntegralNumber()) {
      throw new IllegalArgumentException("Missing or non-integer 'rev' field.");
    }
    return new ConfigVersion(epoch, revNode.longValue());
  }
}
