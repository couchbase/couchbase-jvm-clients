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

import com.couchbase.client.core.error.InvalidArgumentException;
import org.junit.jupiter.api.Test;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class BackoffTest {

  @Test
  void toStringZero() {
    assertEquals("Backoff{ZERO}", Backoff.zero().toString());
  }

  @Test
  void toStringFixed() {
    assertEquals("Backoff{fixed=123ms}", Backoff.fixed(Duration.ofMillis(123)).toString());
  }

  @Test
  void toStringExponentialWithMax() {
    assertEquals(
      "Backoff{exponential,min=1ms,max=123ms,factor=8,basedOnPreviousValue=false}",
      Backoff.exponential(Duration.ofMillis(1), Duration.ofMillis(123), 8, false).toString()
    );
  }

  @Test
  void toStringExponentialNoMax() {
    assertEquals(
      "Backoff{exponential,min=1ms,max=NONE,factor=8,basedOnPreviousValue=false}",
      Backoff.exponential(Duration.ofMillis(1), null, 8, false).toString());
  }

  @Test
  void toStringExponentialWithMaxDependsPrevious() {
    assertEquals(
      "Backoff{exponential,min=1ms,max=123ms,factor=8,basedOnPreviousValue=true}",
      Backoff.exponential(Duration.ofMillis(1), Duration.ofMillis(123), 8, true).toString()
    );
  }

  @Test
  void toStringExponentialNoMaxDependsPrevious() {
    assertEquals(
      "Backoff{exponential,min=1ms,max=NONE,factor=8,basedOnPreviousValue=true}",
      Backoff.exponential(Duration.ofMillis(1), null, 8, true).toString()
    );
  }

  @Test
  void exponentialDoesntThrowArithmeticException_explicitMax() {
    final Duration EXPLICIT_MAX = Duration.ofSeconds(100_000);
    final Duration INIT = Duration.ofSeconds(10);

    Backoff backoff = Backoff.exponential(INIT, EXPLICIT_MAX, 2, false);

    BackoffDelay delay = null;
    IterationContext<String> context = null;
    for (int i = 0; i < 71; i++) {
      if (i == 0) {
        delay = new BackoffDelay(INIT, EXPLICIT_MAX, INIT);
      }
      else {
        context = new DefaultContext<>(null, i, delay, null);
        delay = backoff.apply(context);
      }
    }

    assertNotNull(context);
    assertEquals(delay.delay, EXPLICIT_MAX);
    assertEquals(context.iteration(), 70);
    assertEquals(context.backoff(), EXPLICIT_MAX);
  }

  @Test
  void exponentialDoesntThrowArithmeticException_noSpecificMax() {
    final Duration INIT = Duration.ofSeconds(10);
    final Duration EXPECTED_MAX = Duration.ofSeconds(Long.MAX_VALUE);

    Backoff backoff = Backoff.exponential(INIT, null, 2, false);

    BackoffDelay delay = null;
    IterationContext<String> context = null;
    for (int i = 0; i < 71; i++) {
      if (i == 0) {
        delay = new BackoffDelay(INIT, null, INIT);
      }
      else {
        context = new DefaultContext<>(null, i, delay, null);
        delay = backoff.apply(context);
      }
    }

    assertNotNull(context);
    assertEquals(delay.delay, EXPECTED_MAX);
    assertEquals(context.iteration(), 70);
    assertEquals(context.backoff(), EXPECTED_MAX);
  }

  @Test
  void exponentialDoesntThrowArithmeticException_explicitMaxDependsOnPrevious() {
    final Duration EXPLICIT_MAX = Duration.ofSeconds(100_000);
    final Duration INIT = Duration.ofSeconds(10);

    Backoff backoff = Backoff.exponential(INIT, EXPLICIT_MAX, 2, true);

    BackoffDelay delay = null;
    IterationContext<String> context = null;
    for (int i = 0; i < 71; i++) {
      if (i == 0) {
        delay = new BackoffDelay(INIT, EXPLICIT_MAX, INIT);
      }
      else {
        context = new DefaultContext<>(null, i, delay, null);
        delay = backoff.apply(context);
      }
    }

    assertNotNull(context);
    assertEquals(delay.delay, EXPLICIT_MAX);
    assertEquals(context.iteration(), 70);
    assertEquals(context.backoff(), EXPLICIT_MAX);
  }

  @Test
  void exponentialDoesntThrowArithmeticException_noSpecificMaxDependsOnPrevious() {
    final Duration INIT = Duration.ofSeconds(10);
    final Duration EXPECTED_MAX = Duration.ofSeconds(Long.MAX_VALUE);

    Backoff backoff = Backoff.exponential(INIT, null, 2, true);

    BackoffDelay delay = null;
    IterationContext<String> context = null;
    for (int i = 0; i < 71; i++) {
      if (i == 0) {
        delay = new BackoffDelay(INIT, null, INIT);
      }
      else {
        context = new DefaultContext<>(null, i, delay, null);
        delay = backoff.apply(context);
      }
    }

    assertNotNull(context);
    assertEquals(delay.delay, EXPECTED_MAX);
    assertEquals(context.iteration(), 70);
    assertEquals(context.backoff(), EXPECTED_MAX);
  }

  @Test
  void exponentialRejectsMaxLowerThanFirst() {
    InvalidArgumentException ex = assertThrows(InvalidArgumentException.class, () -> Backoff.exponential(Duration.ofSeconds(2), Duration.ofSeconds(1), 1, false));
    assertEquals("maxBackoff must be >= firstBackoff", ex.getMessage());
    ex = assertThrows(InvalidArgumentException.class, () -> Backoff.exponential(Duration.ofSeconds(2), Duration.ofSeconds(1), 1, true));
    assertEquals("maxBackoff must be >= firstBackoff", ex.getMessage());
  }

  @Test
  public void exponentialAcceptsMaxEqualToFirst() {
    assertDoesNotThrow(() -> Backoff.exponential(Duration.ofSeconds(1), Duration.ofSeconds(1), 1, false));
    assertDoesNotThrow(() -> Backoff.exponential(Duration.ofSeconds(1), Duration.ofSeconds(1), 1, true));
  }

}