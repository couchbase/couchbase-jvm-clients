package com.couchbase.client.core.util;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


public class CbDurationsTest {

  @Test
  void shouldReturnSameValueForWholeSeconds() {
    assertEquals(0, CbDurations.getSecondsCeil(Duration.ZERO));
    assertEquals(1, CbDurations.getSecondsCeil(Duration.ofSeconds(1)));
    assertEquals(5, CbDurations.getSecondsCeil(Duration.ofSeconds(5)));
    assertEquals(Integer.MAX_VALUE,
      CbDurations.getSecondsCeil(Duration.ofSeconds(Integer.MAX_VALUE)));
  }

  @Test
  void shouldRoundUpForPartialSeconds() {
    assertEquals(1, CbDurations.getSecondsCeil(Duration.ofMillis(1)));
    assertEquals(1, CbDurations.getSecondsCeil(Duration.ofMillis(999)));
    assertEquals(2, CbDurations.getSecondsCeil(Duration.ofMillis(1001)));
    assertEquals(2, CbDurations.getSecondsCeil(Duration.ofNanos(1_000_000_001)));
  }

  @Test
  void shouldHandleMixedValues() {
    assertEquals(3, CbDurations.getSecondsCeil(
      Duration.ofSeconds(2).plusMillis(500)));
    assertEquals(3, CbDurations.getSecondsCeil(
      Duration.ofSeconds(2).plusNanos(1)));
  }
}
