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
import com.couchbase.client.core.api.kv.CoreSubdocMutateResult;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.codec.TypeRef;

/**
 * This result is returned from successful KeyValue subdocument mutation responses.
 *
 * @since 3.0.0
 */
public class MutateInResult extends MutationResult {

  /**
   * Holds the encoded subdoc responses.
   */
  private final CoreSubdocMutateResult core;

  /**
   * The default JSON serializer that should be used.
   */
  private final JsonSerializer serializer;

  @Stability.Internal
  public MutateInResult(final CoreSubdocMutateResult core, JsonSerializer serializer) {
    super(core.cas(), core.mutationToken());
    this.core = core;
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
    return serializer.deserialize(target, core.field(index).value());
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
    return serializer.deserialize(target, core.field(index).value());
  }
}
