/*
 * Copyright 2026 Couchbase, Inc.
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

package com.couchbase.client.core.service;

import com.couchbase.client.core.cnc.events.io.UnknownServerPushRequestReceivedEvent;
import com.couchbase.client.core.endpoint.EndpointContext;
import com.couchbase.client.core.io.KeyValuePacket;
import com.couchbase.client.core.io.SectionReader;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.topology.TopologyRevision;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.couchbase.client.core.util.CbStrings.emptyToNull;

/**
 * Handles the requests a server pushes to a KV connection.
 */
final class KeyValueServerPush {
  private static final Logger log = LoggerFactory.getLogger(KeyValueServerPush.class);

  private KeyValueServerPush() {
  }

  /**
   * Returns true if the packet is a request from the server (which {@link #handleServerPush} handles), not a response.
   */
  static boolean isServerPush(KeyValuePacket packet) {
    return packet.magic() == MemcacheProtocol.Magic.SERVER_PUSH_REQUEST.magic();
  }

  /**
   * Handles a request from the server.
   */
  static void handleServerPush(EndpointContext endpointContext, KeyValuePacket packet) {
    byte opcodeByte = packet.opcode();
    MemcacheProtocol.ServerPushOpcode opcode = MemcacheProtocol.ServerPushOpcode.of(opcodeByte);
    if (opcode == MemcacheProtocol.ServerPushOpcode.CLUSTERMAP_CHANGE_NOTIFICATION) {
      String bucketOrNullIfGlobal = emptyToNull(packet.keyAsString());
      TopologyRevision newRevision = parseTopologyRevision(packet);
      endpointContext.core().configurationProvider().signalNewTopologyAvailable(
        bucketOrNullIfGlobal,
        newRevision,
        endpointContext.remoteSocket()
      );
      return;
    }

    endpointContext.core().environment().eventBus().publish(
      new UnknownServerPushRequestReceivedEvent(null, packet.toByteArray())
    );
  }

  private static @Nullable TopologyRevision parseTopologyRevision(KeyValuePacket packet) {
    SectionReader extras = packet.extras();
    switch (extras.remaining()) {
      case 4:
        return new TopologyRevision(0, extras.readUnsignedInt());
      case 16:
        return new TopologyRevision(extras.readLong(), extras.readLong());
      default:
        log.debug("Unexpected CLUSTERMAP_CHANGE_NOTIFICATION extras length: {}", extras.remaining());
        return null;
    }
  }
}
