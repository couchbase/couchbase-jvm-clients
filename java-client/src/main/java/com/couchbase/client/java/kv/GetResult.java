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

import com.couchbase.client.java.codec.Decoder;
import com.couchbase.client.java.codec.DefaultDecoder;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;

import java.time.Duration;
import java.util.Optional;

/**
 * Returned from all kinds of KeyValue Get operation to fetch a document or a subset of it.
 *
 * @since 3.0.0
 */
public class GetResult {

  /**
   * Holds the fetched document in an encoded form.
   */
  private final EncodedDocument encoded;

  /**
   * The CAS of the fetched document.
   */
  private final long cas;

  /**
   * The expiration if fetched and present.
   */
  private final Optional<Duration> expiration;

  /**
   * Creates a new {@link GetResult}.
   *
   * @param encoded the loaded document in encoded form.
   * @param cas the cas from the doc.
   * @param expiration the expiration if fetched from the doc.
   */
  GetResult(final EncodedDocument encoded, final long cas, final Optional<Duration> expiration) {
    this.cas = cas;
    this.encoded = encoded;
    this.expiration = expiration;
  }

  /**
   * Returns the CAS value of the loaded document.
   */
  public long cas() {
    return cas;
  }

  /**
   * If present, returns the expiration of the loaded document.
   *
   * <p>Note that the duration represents the time when the document has been loaded and can only
   * ever be an approximation.</p>
   */
  public Optional<Duration> expiration() {
    return expiration;
  }

  /**
   * Decodes the content of the document into a {@link JsonObject}.
   */
  public JsonObject contentAsObject() {
    return contentAs(JsonObject.class);
  }

  /**
   * Decodes the content of the document into a {@link JsonArray}.
   */
  public JsonArray contentAsArray() {
    return contentAs(JsonArray.class);
  }

  /**
   * Decodes the content of the document into a the target class using the default decoder.
   *
   * <p>Note that while the decoder understands many types, if it fails to decode a certain POJO
   * or custom decoding is needed use the {@link #contentAs(Class, Decoder)} overload.</p>
   *
   * @param target the target class to decode the encoded content into.
   */
  @SuppressWarnings({ "unchecked" })
  public <T> T contentAs(final Class<T> target) {
    return contentAs(target, (Decoder<T>) DefaultDecoder.INSTANCE);
  }

  /**
   * Decodes the content of the document into a the target class using a custom decoder.
   *
   * @param target the target class to decode the encoded content into.
   * @param decoder the decoder that should be used to decode the content.
   */
  public <T> T contentAs(final Class<T> target, final Decoder<T> decoder) {
    return decoder.decode(target, encoded);
  }

  @Override
  public String toString() {
    return "GetResult{" +
      "encoded=" + encoded +
      ", cas=" + cas +
      ", expiration=" + expiration +
      '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    GetResult getResult = (GetResult) o;

    if (cas != getResult.cas) return false;
    if (encoded != null ? !encoded.equals(getResult.encoded) : getResult.encoded != null)
      return false;
    return expiration != null ? expiration.equals(getResult.expiration) : getResult.expiration == null;
  }

  @Override
  public int hashCode() {
    int result = encoded != null ? encoded.hashCode() : 0;
    result = 31 * result + (int) (cas ^ (cas >>> 32));
    result = 31 * result + (expiration != null ? expiration.hashCode() : 0);
    return result;
  }

}
