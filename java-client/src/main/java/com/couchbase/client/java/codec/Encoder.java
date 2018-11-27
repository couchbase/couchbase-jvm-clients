package com.couchbase.client.java.codec;

import com.couchbase.client.java.kv.EncodedDocument;

public interface Encoder<T> {

  EncodedDocument encode(T input);

}
