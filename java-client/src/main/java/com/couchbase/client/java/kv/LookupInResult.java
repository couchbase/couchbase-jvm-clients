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

import com.couchbase.client.core.msg.kv.SubdocField;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.codec.Serializer;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

/**
 * This result is returned from successful KeyValue subdocument lookup responses.
 *
 * @since 3.0.0
 */
public class LookupInResult {

  /**
   * Holds the encoded subdoc responses.
   */
  private final List<SubdocField> encoded;

  /**
   * Holds the cas of the response.
   */
  private final long cas;

  /**
   * The expiration if fetched and present.
   */
  private final Optional<Duration> expiration;

  /**
   * The default JSON serializer that should be used.
   */
  private final Serializer serializer;

  /**
   * Creates a new {@link LookupInResult}.
   *
   * @param encoded the encoded subdoc fields.
   * @param cas the cas of the outer doc.
   */
  LookupInResult(final List<SubdocField> encoded, final long cas, final Optional<Duration> expiration, Serializer serializer) {
    this.cas = cas;
    this.encoded = encoded;
    this.expiration = expiration;
    this.serializer = serializer;
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
   * Returns the CAS value of the document.
   */
  public long cas() {
    return cas;
  }

  // TODO these should return null if exists is false

  /**
   * Decodes the content at the given index into the target class with the default decoder.
   *
   * @param index the index of the subdoc value to decode.
   * @param target the target type to decode into.
   * @return the decoded content into the generic type requested.
   */
  @SuppressWarnings({ "unchecked" })
  public <T> T contentAs(int index, final Class<T> target) {
    return contentAs(index, target, serializer);
  }

  /**
   * Decodes the content at the given index into the target class with a custom decoder.
   *
   * @param index the index of the subdoc value to decode.
   * @param target the target type to decode into.
   * @param serializer the custom serializer that will be used.
   * @return the decoded content into the generic type requested.
   */
  public <T> T contentAs(int index, final Class<T> target, final Serializer serializer) {
    if (index >= 0 && index < encoded.size()) {
      SubdocField value = encoded.get(index);
      value.error().map(err -> {
        throw err;
      });
      return serializer.deserialize(target, value.value());
    } else {
      throw new IllegalArgumentException("Index " + index + " is invalid");
    }
  }

  /**
   * Decodes the encoded content at the given index into a {@link JsonObject}.
   *
   * @param index the index at which to decode.
   */
  public JsonObject contentAsObject(int index) {
    return contentAs(index, JsonObject.class);
  }

  /**
   * Decodes the encoded content at the given index into a {@link JsonArray}.
   *
   * @param index the index at which to decode.
   */
  public JsonArray contentAsArray(int index) {
    return contentAs(index, JsonArray.class);
  }

  /**
   * Allows to check if a value at the given index exists.
   *
   * @param index the index at which to check.
   * @return true if a value is present at the index, false otherwise.
   */
  public boolean exists(int index) {
    if (index >= 0 && index < encoded.size()) {
      SubdocField value = encoded.get(index);
      return value.status().success();
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return "LookupInResult{" +
      "encoded=" + encoded +
      ", cas=" + cas +
      '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    LookupInResult that = (LookupInResult) o;

    if (cas != that.cas) return false;
    return encoded != null ? encoded.equals(that.encoded) : that.encoded == null;
  }

  @Override
  public int hashCode() {
    int result = encoded != null ? encoded.hashCode() : 0;
    result = 31 * result + (int) (cas ^ (cas >>> 32));
    return result;
  }
}
