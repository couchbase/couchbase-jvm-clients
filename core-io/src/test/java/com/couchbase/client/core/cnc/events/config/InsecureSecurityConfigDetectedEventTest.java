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

package com.couchbase.client.core.cnc.events.config;

import com.couchbase.client.core.cnc.Event;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Verifies the output of the {@link InsecureSecurityConfigDetectedEvent}.
 */
class InsecureSecurityConfigDetectedEventTest {

  @Test
  void verifyDescription() {
    String prefix = "Detected a potentially insecure SecurityConfig - Reason: ";

    Event event = new InsecureSecurityConfigDetectedEvent(false, false);
    assertEquals(prefix + "hostname validation is disabled", event.description());

    event = new InsecureSecurityConfigDetectedEvent(true, true);
    assertEquals(prefix + "the InsecureTrustManager is used", event.description());

    event = new InsecureSecurityConfigDetectedEvent(false, true);
    assertEquals(prefix + "hostname validation is disabled and the InsecureTrustManager is used", event.description());
  }

}