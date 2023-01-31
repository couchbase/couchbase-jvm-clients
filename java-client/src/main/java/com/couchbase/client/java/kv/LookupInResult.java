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
import com.couchbase.client.core.api.kv.CoreSubdocGetResult;
import com.couchbase.client.core.error.CasMismatchException;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.msg.kv.SubDocumentField;
import com.couchbase.client.core.msg.kv.SubdocCommandType;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.codec.TypeRef;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * This result is returned from successful KeyValue subdocument lookup responses.
 *
 * @since 3.0.0
 */
public class LookupInResult {

  private final CoreSubdocGetResult core;

  /**
   * The default JSON serializer that should be used.
   */
  private final JsonSerializer serializer;

  @Stability.Internal
  public LookupInResult(CoreSubdocGetResult core, JsonSerializer serializer) {
    this.core = requireNonNull(core);
    this.serializer = requireNonNull(serializer);
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
    return core.cas();
  }

  /**
   * Decodes the content at the given index into an instance of the target class.
   *
   * @param index the index of the subdoc value to decode.
   * @param target the target type to decode into.
   * @return the decoded content into the generic type requested.
   */
  public <T> T contentAs(int index, final Class<T> target) {
    return serializer.deserialize(target, contentAsBytes(index));
  }

  /**
   * Decodes the content at the given index into an instance of the target type.
   *
   * @param index the index of the subdoc value to decode.
   * @param target the target type to decode into.
   * @return the decoded content into the generic type requested.
   */
  public <T> T contentAs(int index, final TypeRef<T> target) {
    return serializer.deserialize(target, contentAsBytes(index));
  }

  /**
   * Returns the raw JSON bytes of the content at the given index.
   * <p>
   * Note that if the field is a string then it will be surrounded by quotation marks, as this is the raw response from
   * the server.  E.g. "foo" will return a 5-byte array.
   *
   * @param index the index of the subdoc value to retrieve.
   * @return the JSON content as a byte array
   */
  @Stability.Uncommitted
  public byte[] contentAsBytes(int index) {
    SubDocumentField field = core.field(index);

    if (field.type() == SubdocCommandType.EXISTS) {
      boolean exists = core.exists(index);

      // This `if` block is for bug-compatibility with JCBC-2056.
      // Remove it when we're ready to change that behavior.
      if (!exists) {
        field.throwErrorIfPresent();
      }

      return String.valueOf(exists).getBytes(UTF_8);
    }

    field.throwErrorIfPresent();
    return field.value();
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
    try {
      return core.exists(index);
    } catch (CouchbaseException e) {
      // For compatibility with previous behavior, treat "invalid index"
      // and "indeterminate result" as "does not exist".
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
    return core.tombstone();
  }

  @Override
  public String toString() {
    return "LookupInResult{" +
      "core=" + core +
      ", serializer=" + serializer +
      '}';
  }
}
