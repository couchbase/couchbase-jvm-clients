package com.couchbase.client.java.codec;

import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.java.json.JacksonTransformers;
import com.couchbase.client.java.kv.EncodedDocument;

public class DefaultEncoder implements Encoder {

  public static final DefaultEncoder INSTANCE = new DefaultEncoder();

  @Override
  public EncodedDocument encode(final Object input) {
    try {
      int flags = 0; // TODO: FIXME
      byte[] encoded;
      if (input instanceof BinaryContent) {
        encoded = ((BinaryContent) input).content();
      } else {
        encoded = JacksonTransformers.MAPPER.writeValueAsBytes(input); // FIXME
      }
      return new EncodedDocument(flags, encoded);
    } catch (Exception ex) {
      // TODO: better hierachy
      throw new CouchbaseException(ex);
    }
  }
}
