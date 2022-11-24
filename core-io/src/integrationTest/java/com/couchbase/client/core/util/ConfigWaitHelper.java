/*
 * Copyright (c) 2022 Couchbase, Inc.
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
package com.couchbase.client.core.util;

import com.couchbase.client.core.cnc.EventBus;
import com.couchbase.client.core.cnc.SimpleEventBus;
import com.couchbase.client.core.cnc.events.core.ReconfigurationCompletedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/*
 * There is a tricky race that these tests intermittently hit:
 *
 * The test starts a loader, e.g. GlobalLoader, and tells it to load config from a seed node.
 * It starts doing so.  It adds the seed node to the nodes list.  And creates a config request (e.g. CarrierGlobalConfigRequest), and tries to send it to that node.
 * This goes into dispatchTargeted, where the nodes list is present, but the node is in CONNECTING state.  So, it goes to handleTargetNotAvailable.
 * But while all this has been going on, DefaultConfigurationProvider has been emits an empty config.
 * But now, the empty config emitted by DefaultConfigurationProvider propagates through, and the nodes list is emptied.
 * handleTargetNotAvailable handles this by raising a RequestCanceledException with TARGET_NODE_REMOVED, and the test fails.
 *
 * There are other race cases that present different symptoms but have the same root cause.
 *
 * The solution: just wait for the initial empty config to propagate through, first.
 */
public class ConfigWaitHelper {
  private final Logger logger = LoggerFactory.getLogger(ConfigWaitHelper.class);
  private final CountDownLatch latch = new CountDownLatch(1);

  public ConfigWaitHelper(EventBus eventBus) {
    if (eventBus instanceof SimpleEventBus) {
      SimpleEventBus seb = (SimpleEventBus) eventBus;

      new Thread(() -> {
        while (true) {
          if (seb.publishedEvents().stream().anyMatch(v -> v instanceof ReconfigurationCompletedEvent)) {
            latch.countDown();
            break;
          }
          try {
            Thread.sleep(10);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
      }).start();
    }
    else {
      eventBus.subscribe(event -> {
        // This event happens after core has finished processing the config.
        if (event instanceof ReconfigurationCompletedEvent) {
          latch.countDown();
        }
      });
    }
  }

  public void await() {
    logger.info("Waiting for initial empty config");

    try {
      if (!latch.await(10, TimeUnit.SECONDS)) {
        throw new RuntimeException("Timeout while waiting for initial empty config");
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    logger.info("Received initial empty config");
  }
}
