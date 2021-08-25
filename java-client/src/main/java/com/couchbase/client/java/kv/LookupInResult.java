/*
 * Copyright (c) 2020 Couchbase, Inc.
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
import com.couchbase.client.core.error.CasMismatchException;
import com.couchbase.client.core.error.context.KeyValueErrorContext;
import com.couchbase.client.core.error.subdoc.PathInvalidException;
import com.couchbase.client.core.error.context.SubDocumentErrorContext;
import com.couchbase.client.core.msg.kv.SubDocumentField;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.codec.TypeRef;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;

import java.util.Arrays;
import java.util.Objects;

/**
 * This result is returned from successful KeyValue subdocument lookup responses.
 *
 * @since 3.0.0
 */
public class LookupInResult {

  /**
   * Holds the encoded subdoc responses.
   */
  private final SubDocumentField[] encoded;

  /**
   * Holds the cas of the response.
   */
  private final long cas;

  /**
   * The default JSON serializer that should be used.
   */
  private final JsonSerializer serializer;

  /**
   * The higher level kv error context if present.
   */
  private final KeyValueErrorContext ctx;

  /**
   * Whether this is a deleted document (tombstone).
   */
  private final boolean isDeleted;

  /**
   * Creates a new {@link LookupInResult}.
   *
   * @param encoded the encoded subdoc fields.
   * @param cas the cas of the outer doc.
   */
  LookupInResult(final SubDocumentField[] encoded,
                 final long cas,
                 JsonSerializer serializer,
                 final KeyValueErrorContext ctx,
                 final boolean isDeleted) {
    this.cas = cas;
    this.encoded = encoded;
    this.serializer = serializer;
    this.ctx = ctx;
    this.isDeleted = isDeleted;
  }

  /**
   * Returns the CAS value of document at the time of loading.
   * <p>
   * The CAS value is an opaque identifier which is associated with a specific state of the document on the server. It
   * can be used during a subsequent mutation to make sure that the document has not been modified in the meantime.
   * <p>
   * If document on the server has been modified in the meantime the SDK will raise a {@link CasMismatchException}. In
   * this case the caller is expected to re-do the whole "fetch-modify-update" cycle again. Please refer to the
   * SDK documentation for more information on CAS mismatches and subsequent retries.
   */
  public long cas() {
    return cas;
  }

  /**
   * Decodes the content at the given index into an instance of the target class.
   *
   * @param index the index of the subdoc value to decode.
   * @param target the target type to decode into.
   * @return the decoded content into the generic type requested.
   */
  public <T> T contentAs(int index, final Class<T> target) {
    return serializer.deserialize(target, getFieldAtIndex(index).value());
  }

  /**
   * Decodes the content at the given index into an instance of the target type.
   *
   * @param index the index of the subdoc value to decode.
   * @param target the target type to decode into.
   * @return the decoded content into the generic type requested.
   */
  public <T> T contentAs(int index, final TypeRef<T> target) {
    return serializer.deserialize(target, getFieldAtIndex(index).value());
  }

  /**
   * Returns the raw JSON bytes of the content at the given index.
   *
   * @param index the index of the subdoc value to retrieve.
   * @return the JSON content as a byte array
   */
  @Stability.Uncommitted
  public byte[] contentAsBytes(int index) {
    return getFieldAtIndex(index).value();
  }

  private SubDocumentField getFieldAtIndex(int index) {
    if (index >= 0 && index < encoded.length) {
      SubDocumentField value = encoded[index];
      if (value == null) {
        throw new PathInvalidException(
          "No result exists at index",
          new SubDocumentErrorContext(ctx, index, null, null)
        );
      }
      if (value.error().isPresent()) {
        throw value.error().get();
      }
      return value;
    } else {
      throw new PathInvalidException(
        "Index is out of bounds",
        new SubDocumentErrorContext(ctx, index, null, null)
      );
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
    if (index >= 0 && index < encoded.length) {
      SubDocumentField value = encoded[index];
      return value != null && value.status().success();
    } else {
      return false;
    }
  }

  /**
   * Returns whether this document was deleted (a tombstone).
   * <p>
   * Will always be false unless {@link LookupInOptions#accessDeleted(boolean)} has been set.
   * <p>
   * For internal use only: applications should not require it.
   *
   * @return whether this document was a tombstone
   */
  @Stability.Internal
  public boolean isDeleted() {
    return isDeleted;
  }

  @Override
  public String toString() {
    return "LookupInResult{" +
      "encoded=" + Arrays.asList(encoded) +
      ", cas=0x" + Long.toHexString(cas) +
      ", isDeleted=" + isDeleted +
      '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    LookupInResult that = (LookupInResult) o;
    return cas == that.cas && Arrays.equals(encoded, that.encoded) && Objects.equals(serializer, that.serializer) && isDeleted == that.isDeleted;
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(cas, serializer, isDeleted);
    result = 31 * result + Arrays.hashCode(encoded);
    return result;
  }
}
