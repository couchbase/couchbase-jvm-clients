package com.couchbase.client.java.codec;

import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.java.kv.EncodedDocument;

public class DefaultDecoder implements Decoder<Object> {

  public static final DefaultDecoder INSTANCE = new DefaultDecoder();

  @Override
  public Object decode(Class<Object> target, EncodedDocument encoded) {
    return Mapper.decodeInto(encoded.content(), target); // TODO: fixme based on flags
  }

}
