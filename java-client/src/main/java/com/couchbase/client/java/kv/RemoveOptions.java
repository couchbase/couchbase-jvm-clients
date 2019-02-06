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
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.CommonOptions;

import java.time.Duration;
import java.util.Optional;

import static com.couchbase.client.core.util.Validators.notNull;

public class RemoveOptions extends CommonDurabilityOptions<RemoveOptions> {

  public static RemoveOptions DEFAULT = new RemoveOptions();

  private Duration expiry;
  private long cas;

  private RemoveOptions() { }

  public static RemoveOptions removeOptions() {
    return new RemoveOptions();
  }

  public RemoveOptions expiry(final Duration expiry) {
    this.expiry = expiry;
    return this;
  }

  public RemoveOptions cas(long cas) {
    this.cas = cas;
    return this;
  }


  @Stability.Internal
  public BuiltRemoveOptions build() {
    return new BuiltRemoveOptions();
  }

  public class BuiltRemoveOptions extends BuiltCommonDurabilityOptions {

    public long cas() {
      return cas;
    }

    public Duration expiry() {
      return expiry;
    }
  }
}
