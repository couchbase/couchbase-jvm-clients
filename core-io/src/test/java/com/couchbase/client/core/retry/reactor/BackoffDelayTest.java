/*
 * Copyright (c) 2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.retry.reactor;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class BackoffDelayTest {

  @Test
  void toStringZero() {
    assertEquals("{ZERO}", BackoffDelay.ZERO.toString());
  }

  @Test
  void toStringExhausted() {
    assertEquals("{EXHAUSTED}", AbstractRetry.RETRY_EXHAUSTED.toString());
  }

  @Test
  void toStringSimple() {
    assertEquals("{3000ms}", new BackoffDelay(Duration.ofSeconds(3)).toString());
  }

  @Test
  void toStringMinMaxDelay() {
    assertEquals("{123ms/4000ms}", new BackoffDelay(
      Duration.ofSeconds(3),
      Duration.ofSeconds(4),
      Duration.ofMillis(123)
    ).toString());
  }
}