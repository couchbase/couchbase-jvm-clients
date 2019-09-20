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
import com.couchbase.client.java.codec.JsonSerializer;

import java.time.Duration;

import static com.couchbase.client.core.util.Validators.notNull;

public class MutateInOptions extends CommonDurabilityOptions<MutateInOptions> {

  private Duration expiry = Duration.ZERO;
  private long cas = 0;
  private StoreSemantics storeSemantics = StoreSemantics.REPLACE;
  private JsonSerializer serializer = null;
  private boolean accessDeleted = false;

  public static MutateInOptions mutateInOptions() {
    return new MutateInOptions();
  }

  private MutateInOptions() {
  }

  public MutateInOptions expiry(final Duration expiry) {
    this.expiry = expiry;
    return this;
  }

  public MutateInOptions cas(long cas) {
    this.cas = cas;
    return this;
  }

  public MutateInOptions serializer(final JsonSerializer serializer) {
    notNull(serializer, "Serializer");
    this.serializer = serializer;
    return this;
  }

  public MutateInOptions storeSemantics(final StoreSemantics storeSemantics) {
    notNull(storeSemantics, "StoreSemantics");
    this.storeSemantics = storeSemantics;
    return this;
  }

  /**
   * For internal use only: allows access to deleted documents that are in 'tombstone' form.
   */
  @Stability.Internal
  public MutateInOptions accessDeleted(final boolean accessDeleted) {
    notNull(accessDeleted, "AccessDeleted");
    this.accessDeleted = accessDeleted;
    return this;
  }

  @Stability.Internal
  public Built build() {
    return new Built();
  }

  public class Built extends BuiltCommonDurabilityOptions {

    public Duration expiry() {
      return expiry;
    }

    public long cas() {
      return cas;
    }

    public StoreSemantics storeSemantics() {
      return storeSemantics;
    }

    public JsonSerializer serializer() {
      return serializer;
    }

    public boolean accessDeleted() {
      return accessDeleted;
    }
  }
}
