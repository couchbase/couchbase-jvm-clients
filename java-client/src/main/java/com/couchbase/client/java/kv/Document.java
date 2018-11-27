/*
 * Copyright (c) 2018 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.java.kv;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.java.codec.Decoder;
import com.couchbase.client.java.codec.DefaultDecoder;
import com.couchbase.client.java.codec.DefaultEncoder;
import com.couchbase.client.java.codec.Encoder;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;

import java.time.Duration;
import java.util.Optional;

/**
 * Experimental prototype for a different result type on fetch.
 */
public class Document {

  private final String id;
  private final EncodedDocument encoded;
  private final Optional<Long> cas;
  private final Optional<Duration> expiration;

  public static <T> Document create(final String id, final T content) {
    return create(id, content, DefaultEncoder.INSTANCE);
  }

  public static <T> Document create(final String id, final T content, final Encoder<T> encoder) {
    EncodedDocument encoded = encoder.encode(content);
    return new Document(id, encoded, Optional.empty(), Optional.empty());
  }

  public static Document fromEncoded(final String id, final EncodedDocument encoded,
                                     final Optional<Long> cas, final Optional<Duration> expiration)
  {
    return new Document(id, encoded, cas, expiration);
  }

  private Document(final String id, final EncodedDocument encoded, final Optional<Long> cas,
                   final Optional<Duration> expiration) {
    this.id = id;
    this.cas = cas;
    this.encoded = encoded;
    this.expiration = expiration;
  }

  public String id() {
    return id;
  }

  public Optional<Long> cas() {
    return cas;
  }

  public Optional<Duration> expiration() {
    return expiration;
  }

  public JsonObject contentAsObject() {
    return contentAs(JsonObject.class);
  }

  public JsonArray contentAsArray() {
    return contentAs(JsonArray.class);
  }

  @SuppressWarnings({ "unchecked" })
  public <T> T contentAs(final Class<T> target) {
    return contentAs(target, (Decoder<T>) DefaultDecoder.INSTANCE);
  }

  public <T> T contentAs(final Class<T> target, final Decoder<T> decoder) {
    return decoder.decode(target, encoded);
  }

  @Stability.Uncommitted
  public EncodedDocument encoded() {
    return encoded;
  }
}
