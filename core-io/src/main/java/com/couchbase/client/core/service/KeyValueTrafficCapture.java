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

import com.couchbase.client.core.cnc.events.io.ReadTrafficCapturedEvent;
import com.couchbase.client.core.cnc.events.io.WriteTrafficCapturedEvent;
import com.couchbase.client.core.io.IoContext;
import com.couchbase.client.core.io.KeyValuePacket;
import com.couchbase.client.core.io.TrafficCaptureFormat;
import org.jspecify.annotations.NullMarked;

import static java.util.Objects.requireNonNull;

/**
 * Publishes each packet a KV connection writes and reads, if the config asks for KV's
 * (io.servicesToCapture), described by {@link TrafficCaptureFormat}.
 */
@NullMarked
final class KeyValueTrafficCapture {
  private final IoContext ioContext;
  private final boolean enabled;

  KeyValueTrafficCapture(IoContext ioContext) {
    this.ioContext = requireNonNull(ioContext);
    this.enabled = ioContext.environment().ioConfig().servicesToCapture().contains(ServiceType.KV);
  }

  boolean enabled() {
    return enabled;
  }

  /**
   * Publishes the packet about to be written.
   */
  void written(byte[] packet) {
    if (enabled) {
      String description = TrafficCaptureFormat.memcacheToString(KeyValuePacket.wrap(packet, 0, packet.length));
      ioContext.environment().eventBus().publish(new WriteTrafficCapturedEvent(ioContext, description));
    }
  }

  /**
   * Publishes the packet just read.
   */
  void read(KeyValuePacket packet) {
    if (enabled) {
      String description = TrafficCaptureFormat.memcacheToString(packet);
      ioContext.environment().eventBus().publish(new ReadTrafficCapturedEvent(ioContext, description));
    }
  }
}
