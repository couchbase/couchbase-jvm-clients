package com.couchbase.client.core.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CbStringsTest {

  @Test
  @SuppressWarnings({"ConstantExpression", "ObviousNullCheck"})
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
  @SuppressWarnings("ConstantValue")
  void isNullOrEmpty() {
    assertTrue(CbStrings.isNullOrEmpty(null));
    assertTrue(CbStrings.isNullOrEmpty(""));
    assertFalse(CbStrings.isNullOrEmpty("xyzzy"));
  }

  @Test
  @SuppressWarnings("DataFlowIssue")
  void removeStartRejectsNulls() {
    assertThrows(NullPointerException.class, () -> CbStrings.removeStart(null, null));
    assertThrows(NullPointerException.class, () -> CbStrings.removeStart(null, "xyzzy"));
    assertThrows(NullPointerException.class, () -> CbStrings.removeStart("xyzzy", null));
  }

  @Test
  @SuppressWarnings("DataFlowIssue")
  void removeEndRejectsNulls() {
    assertThrows(NullPointerException.class, () -> CbStrings.removeEnd(null, null));
    assertThrows(NullPointerException.class, () -> CbStrings.removeEnd(null, "xyzzy"));
    assertThrows(NullPointerException.class, () -> CbStrings.removeEnd("xyzzy", null));
  }

  @Test
  void removeStart() {
    assertEquals("", CbStrings.removeStart("", ""));
    assertEquals("", CbStrings.removeStart("", "xyzzy"));

    assertEquals("xyzzy", CbStrings.removeStart("xyzzy", ""));
    assertEquals("xyzzy", CbStrings.removeStart("xyzzy", "foo"));
    assertEquals("zzy", CbStrings.removeStart("xyzzy", "xy"));
    assertEquals("", CbStrings.removeStart("xyzzy", "xyzzy"));

    assertEquals("xyzzy", CbStrings.removeStart("xyzzy", "xyzzyxyzzy"));
  }

  @Test
  void removeEnd() {
    assertEquals("", CbStrings.removeEnd("", ""));
    assertEquals("", CbStrings.removeEnd("", "xyzzy"));

    assertEquals("xyzzy", CbStrings.removeEnd("xyzzy", ""));
    assertEquals("xyzzy", CbStrings.removeEnd("xyzzy", "foo"));
    assertEquals("xyz", CbStrings.removeEnd("xyzzy", "zy"));
    assertEquals("", CbStrings.removeEnd("xyzzy", "xyzzy"));

    assertEquals("xyzzy", CbStrings.removeEnd("xyzzy", "xyzzyxyzzy"));
  }
}
