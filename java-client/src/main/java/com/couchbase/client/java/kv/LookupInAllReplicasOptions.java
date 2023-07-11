/*
 * Copyright (c) 2023 Couchbase, Inc.
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
import com.couchbase.client.java.CommonOptions;
import com.couchbase.client.java.codec.JsonSerializer;

import static com.couchbase.client.core.util.Validators.notNull;

public class LookupInAllReplicasOptions extends CommonOptions<LookupInAllReplicasOptions> {
  /**
   * Holds the serializer used for decoding.
   */
  private JsonSerializer serializer;

  public static LookupInAllReplicasOptions lookupInAllReplicasOptions() {
    return new LookupInAllReplicasOptions();
  }

  /**
   * Allows to specify a custom serializer that is used to decode the content of the result.
   *
   * @param serializer the custom serializer that should be used for decoding.
   * @return the {@link LookupInAllReplicasOptions} to allow method chaining.
   */
  public LookupInAllReplicasOptions serializer(final JsonSerializer serializer) {
    notNull(serializer, "Serializer");
    this.serializer = serializer;
    return this;
  }

  private LookupInAllReplicasOptions() {
  }

  @Stability.Internal
  public Built build() {
    return new Built();
  }

  public class Built extends BuiltCommonOptions {

    Built() {
    }

    public JsonSerializer serializer() {
      return serializer;
    }

  }

}
