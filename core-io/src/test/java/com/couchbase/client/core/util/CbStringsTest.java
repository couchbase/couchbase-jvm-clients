package com.couchbase.client.core.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CbStringsTest {

  @Test
  void nullToEmpty() {
    assertEquals("", CbStrings.nullToEmpty(null));
    assertEquals("", CbStrings.nullToEmpty(""));
    assertEquals("xyzzy", CbStrings.nullToEmpty("xyzzy"));
  }

  @Test
  void emptyToNull() {
    assertNull(CbStrings.emptyToNull(null));
    assertNull(CbStrings.emptyToNull(""));
    assertEquals("xyzzy", CbStrings.emptyToNull("xyzzy"));
  }

  @Test
  void isNullOrEmpty() {
    assertTrue(CbStrings.isNullOrEmpty(null));
    assertTrue(CbStrings.isNullOrEmpty(""));
    assertFalse(CbStrings.isNullOrEmpty("xyzzy"));
  }

  @Test
  void removeStart() {
    assertNull(CbStrings.removeStart(null, null));
    assertNull(CbStrings.removeStart(null, ""));
    assertNull(CbStrings.removeStart(null, "xyzzy"));


    assertEquals("", CbStrings.removeStart("", null));
    assertEquals("", CbStrings.removeStart("", ""));
    assertEquals("", CbStrings.removeStart("", "xyzzy"));

    assertEquals("xyzzy", CbStrings.removeStart("xyzzy", null));
    assertEquals("xyzzy", CbStrings.removeStart("xyzzy", ""));
    assertEquals("xyzzy", CbStrings.removeStart("xyzzy", "foo"));
    assertEquals("zzy", CbStrings.removeStart("xyzzy", "xy"));
    assertEquals("", CbStrings.removeStart("xyzzy", "xyzzy"));

    assertEquals("xyzzy", CbStrings.removeStart("xyzzy", "xyzzyxyzzy"));
  }

  @Test
  void removeEnd() {
    assertNull(CbStrings.removeEnd(null, null));
    assertNull(CbStrings.removeEnd(null, ""));
    assertNull(CbStrings.removeEnd(null, "xyzzy"));


    assertEquals("", CbStrings.removeEnd("", null));
    assertEquals("", CbStrings.removeEnd("", ""));
    assertEquals("", CbStrings.removeEnd("", "xyzzy"));

    assertEquals("xyzzy", CbStrings.removeEnd("xyzzy", null));
    assertEquals("xyzzy", CbStrings.removeEnd("xyzzy", ""));
    assertEquals("xyzzy", CbStrings.removeEnd("xyzzy", "foo"));
    assertEquals("xyz", CbStrings.removeEnd("xyzzy", "zy"));
    assertEquals("", CbStrings.removeEnd("xyzzy", "xyzzy"));


    assertEquals("xyzzy", CbStrings.removeEnd("xyzzy", "xyzzyxyzzy"));
  }
}
