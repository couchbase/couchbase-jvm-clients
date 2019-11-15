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

import com.couchbase.client.core.msg.kv.MutationToken;
import com.couchbase.client.core.msg.kv.SubDocumentField;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.codec.TypeRef;

import java.util.NoSuchElementException;
import java.util.Optional;

import static com.couchbase.client.core.logging.RedactableArgument.redactUser;

/**
 * This result is returned from successful KeyValue subdocument mutation responses.
 *
 * @since 3.0.0
 */
public class MutateInResult extends MutationResult {

  /**
   * Holds the encoded subdoc responses.
   */
  private final SubDocumentField[] encoded;

  /**
   * The default JSON serializer that should be used.
   */
  private final JsonSerializer serializer;

  /**
   * Creates a new {@link MutateInResult}.
   *
   * @param encoded the encoded subdoc fields.
   * @param cas the cas of the outer doc.
   * @param mutationToken the mutation token of the doc, if present.
   */
  MutateInResult(final SubDocumentField[] encoded, final long cas, final Optional<MutationToken> mutationToken, JsonSerializer serializer) {
    super(cas, mutationToken);
    this.encoded = encoded;
    this.serializer = serializer;
  }

  /**
   * Decodes the content at the given index into the target class with the default decoder.
   *
   * @param index the index of the subdoc value to decode.
   * @param target the target type to decode into.
   * @return the decoded content into the generic type requested.
   */
  public <T> T contentAs(int index, final Class<T> target) {
    return contentAs(index, target, serializer);
  }

  /**
   * Decodes the content at the given index into an instance of the target type with the default decoder.
   *
   * @param index the index of the subdoc value to decode.
   * @param target the target type to decode into.
   * @return the decoded content into the generic type requested.
   */
  public <T> T contentAs(int index, final TypeRef<T> target) {
    return contentAs(index, target, serializer);
  }

  /**
   * Decodes the content at the given index into an instance of the target class with a custom decoder.
   *
   * @param index the index of the subdoc value to decode.
   * @param target the target type to decode into.
   * @param serializer the custom {@link JsonSerializer} that will be used.
   * @return the decoded content into the generic type requested.
   */
  public <T> T contentAs(int index, final Class<T> target, final JsonSerializer serializer) {
    return serializer.deserialize(target, getFieldAtIndex(index).value());
  }

  /**
   * Decodes the content at the given index into an instance of the target type with a custom decoder.
   *
   * @param index the index of the subdoc value to decode.
   * @param target the target type to decode into.
   * @param serializer the custom {@link JsonSerializer} that will be used.
   * @return the decoded content into the generic type requested.
   */
  public <T> T contentAs(int index, final TypeRef<T> target, final JsonSerializer serializer) {
    return serializer.deserialize(target, getFieldAtIndex(index).value());
  }

  private SubDocumentField getFieldAtIndex(int index) {
    if (index >= 0 && index < encoded.length) {
      SubDocumentField value = encoded[index];
      if (value == null) {
        throw new NoSuchElementException("No result exists at index " + index);
      }
      value.error().map(err -> {
        throw err;
      });
      return value;
    }
    else {
      throw new IllegalArgumentException("Index " + index + " is invalid");
    }
  }

  @Override
  public String toString() {
    return "MutateInResult{" +
      "encoded=" + redactUser(encoded) +
      '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;

    MutateInResult that = (MutateInResult) o;

    return encoded != null ? encoded.equals(that.encoded) : that.encoded == null;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (encoded != null ? encoded.hashCode() : 0);
    return result;
  }
}
