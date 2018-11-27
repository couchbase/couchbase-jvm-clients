package com.couchbase.client.java.codec;

import com.couchbase.client.java.kv.EncodedDocument;

public interface Decoder<T> {

  T decode(Class<T> target, EncodedDocument encoded);

}
