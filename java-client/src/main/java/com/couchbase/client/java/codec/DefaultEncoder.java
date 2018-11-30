package com.couchbase.client.java.codec;

import com.couchbase.client.java.kv.EncodedDocument;

public class DefaultEncoder implements Encoder {

  public static final DefaultEncoder INSTANCE = new DefaultEncoder();

  @Override
  public EncodedDocument encode(final Object input) {
    return null;
  }
}
