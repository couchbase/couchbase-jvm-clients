package com.couchbase.client.core.util;


import com.couchbase.client.core.error.InvalidArgumentException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class GolangTest {

  @Test
  void canParseDuration() {
    // Zero is special case, doesn't need time unit
    checkDuration("PT0S", "0");
    checkDuration("PT0S", "+0");
    checkDuration("PT0S", "-0");

    checkDuration("PT-3H-5M", "-3h5m");
    checkDuration("PT3H", "+3h");
    checkDuration("PT3H", "3h");

    checkDuration("PT2H5M", "1h65m");
    checkDuration("PT32M", ".5h2m");
    checkDuration("PT5H2M", "5.h2m");

    checkDuration("PT5H3M", "0005h0003m"); // leading zeroes are fine

    checkDuration("PT1H2M3.004005006S", "1h2m3s4ms5us6ns");
    checkDuration("PT1H2M3.004005006S", "1h2m3s4ms5µs6ns");

    checkDuration("PT1H15M30.918273645S", "1h15m30.918273645s");
    checkDuration("PT2562047H47M16.854775807S", "9223372036854775807ns");
    checkDuration("PT2562047H47M16.854775807S", "9223372036.854775807s");
    checkDuration("PT1.922337203S", "1.9223372039999s");
    checkDuration("PT59M59.999999996S", ".999999999999h");

    checkDuration("PT3H8M29.733552923S", "3.141592653589793238462643383279h");
    checkDuration("PT393H18M43.141592653S", "1415923.141592653589793238462643383279s");
  }

  @Test
  void bugCompatibleWithGoParseDuration() {
    checkDuration("PT3H2M1S", "1s2m3h"); // components can be in any order
    checkDuration("PT3H7M", "1h2h3m4m"); // or even duplicated.
  }

  @Test
  void enforcesDurationLimits() {
    checkBadDuration("2562048h");
    checkBadDuration("9223372036854775808ns");
    checkBadDuration("14159265353.141592653589793238462643383279s");
  }

  @Test
  void rejectsBadDurationSyntax() {
    checkBadDuration("123");
    checkBadDuration("00");
    checkBadDuration(" 1h");
    checkBadDuration("1h ");
    checkBadDuration("1h 2m");
    checkBadDuration("+-3h");
    checkBadDuration("-+3h");
    checkBadDuration("-");
    checkBadDuration("-.");
    checkBadDuration(".");
    checkBadDuration(".h");
    checkBadDuration("2.3.4h");
    checkBadDuration("3x");
    checkBadDuration("3");
    checkBadDuration("3h4x");
    checkBadDuration("1H");
    checkBadDuration("1h-2m");
  }

  private static void checkBadDuration(String golangDuration) {
    assertThrows(InvalidArgumentException.class, () -> Golang.parseDuration(golangDuration));
  }

  private static void checkDuration(String expected, String golangDuration) {
    assertEquals(expected, Golang.parseDuration(golangDuration).toString());
  }
}
