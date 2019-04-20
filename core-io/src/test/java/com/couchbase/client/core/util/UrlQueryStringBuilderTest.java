package com.couchbase.client.core.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class UrlQueryStringBuilderTest {

  @Test
  void addAndSet() {
    UrlQueryStringBuilder builder = UrlQueryStringBuilder.createForUrlSafeNames()
      .add("color", "red")
      .add("color", "green");
    assertEquals("color=red&color=green", builder.build());

    builder.set("color", "orange");
    assertEquals("color=orange", builder.build());

    builder.add("color", "purple");
    assertEquals("color=orange&color=purple", builder.build());
  }

  @Test
  void onlyValueIsEncoded() {
    UrlQueryStringBuilder builder = UrlQueryStringBuilder.createForUrlSafeNames()
      .add("colör", "réd");
    assertEquals("colör=r%C3%A9d", builder.build());
  }

  @Test
  void nameAndValueAreEncoded() {
    UrlQueryStringBuilder builder = UrlQueryStringBuilder.create()
      .add("colör", "réd");
    assertEquals("col%C3%B6r=r%C3%A9d", builder.build());
  }

}
