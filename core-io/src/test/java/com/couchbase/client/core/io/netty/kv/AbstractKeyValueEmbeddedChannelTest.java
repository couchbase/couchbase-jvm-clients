/*
 * Copyright (c) 2020 Couchbase, Inc.
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

package com.couchbase.client.core.io.netty.kv;

import com.couchbase.client.core.cnc.SimpleEventBus;
import com.couchbase.client.core.deps.io.netty.channel.embedded.EmbeddedChannel;
import com.couchbase.client.core.deps.io.netty.util.ResourceLeakDetector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

/**
 * Useful parent class for KV unit tests using the netty embedded channel.
 */
public abstract class AbstractKeyValueEmbeddedChannelTest {

  static {
    // Makes sure that the embedded channel tests do not leak buffers.
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
  }

  /**
   * Holds the embedded channel that the child classes should use.
   */
  protected EmbeddedChannel channel;

  /**
   * Holds the event bus the child classes should use.
   */
  protected SimpleEventBus eventBus;

  @BeforeEach
  protected void beforeEach() {
    channel = new EmbeddedChannel();
    eventBus = new SimpleEventBus(true);
  }

  @AfterEach
  protected void afterEach() {
    channel.finishAndReleaseAll();
  }

}
