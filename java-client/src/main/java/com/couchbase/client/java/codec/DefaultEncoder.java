package com.couchbase.client.java.codec;

import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.java.kv.EncodedDocument;

public class DefaultEncoder implements Encoder {

  public static final DefaultEncoder INSTANCE = new DefaultEncoder();

  @Override
  public EncodedDocument encode(final Object input) {
    int flags = 0; // TODO: FIXME
    byte[] encoded = Mapper.encodeAsBytes(input); // FIXME
    return new EncodedDocument(flags, encoded);
  }
}
