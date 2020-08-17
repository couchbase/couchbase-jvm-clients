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
import com.couchbase.client.java.CommonOptions;
import com.couchbase.client.java.codec.JsonSerializer;

import static com.couchbase.client.core.util.Validators.notNull;

public class LookupInOptions extends CommonOptions<LookupInOptions> {

  private JsonSerializer serializer;

  private boolean accessDeleted = false;

  public static LookupInOptions lookupInOptions() {
    return new LookupInOptions();
  }

  /**
   * Customizes the serializer that is used to decode the contents of the {@link LookupInResult}.
   *
   * @param serializer the serializer used for decoding.
   * @return this {@link LookupInOptions} for chaining purposes.
   */
  public LookupInOptions serializer(final JsonSerializer serializer) {
    this.serializer = notNull(serializer, "Serializer");
    return this;
  }

  /**
   * For internal use only: allows access to deleted documents that are in 'tombstone' form.
   */
  @Stability.Internal
  public LookupInOptions accessDeleted(boolean accessDeleted) {
    this.accessDeleted = accessDeleted;
    return this;
  }

  private LookupInOptions() {
  }

  @Stability.Internal
  public Built build() {
    return new Built();
  }

  public class Built extends BuiltCommonOptions {

    Built() { }

    public JsonSerializer serializer() {
      return serializer;
    }

    public boolean accessDeleted() {
      return accessDeleted;
    }
  }

}
