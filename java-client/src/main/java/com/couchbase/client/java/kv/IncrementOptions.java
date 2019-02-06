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

import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.CommonOptions;

import java.util.Optional;

import static com.couchbase.client.core.util.Validators.notNull;

public class IncrementOptions extends CommonDurabilityOptions<IncrementOptions> {
  public static IncrementOptions DEFAULT = new IncrementOptions();

  private long delta;
  private Optional<Long> initial;
  private int expiry;

  public static IncrementOptions incrementOptions() {
    return new IncrementOptions();
  }

  private IncrementOptions() {
    delta = 1;
    initial = Optional.empty();
    expiry = 0;
  }

  public long delta() {
    return delta;
  }

  public IncrementOptions delta(long delta) {
    if (delta < 0) {
      throw new IllegalArgumentException("The delta cannot be less than 0");
    }
    this.delta = delta;
    return this;
  }

  public Optional<Long> initial() {
    return initial;
  }

  public IncrementOptions initial(Optional<Long> initial) {
    this.initial = initial;
    return this;
  }

  public int expiry() {
    return expiry;
  }

  public IncrementOptions expiry(int expiry) {
    this.expiry = expiry;
    return this;
  }

}
