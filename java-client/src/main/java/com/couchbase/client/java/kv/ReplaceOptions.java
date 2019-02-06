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
import com.couchbase.client.java.codec.DefaultEncoder;
import com.couchbase.client.java.codec.Encoder;

import java.time.Duration;

import static com.couchbase.client.core.util.Validators.notNull;

public class ReplaceOptions extends CommonDurabilityOptions<ReplaceOptions> {

  public static ReplaceOptions DEFAULT = new ReplaceOptions();

  private Duration expiry = Duration.ZERO;
  private Encoder encoder = DefaultEncoder.INSTANCE;
  private long cas;

  private ReplaceOptions() { }

  public static ReplaceOptions replaceOptions() {
    return new ReplaceOptions();
  }

  public ReplaceOptions expiry(final Duration expiry) {
    this.expiry = expiry;
    return this;
  }

  public ReplaceOptions encoder(final Encoder encoder) {
    notNull(encoder, "Encoder");

    this.encoder = encoder;
    return this;
  }

  public ReplaceOptions cas(long cas) {
    this.cas = cas;
    return this;
  }

  @Stability.Internal
  public BuiltReplaceOptions build() {
    return new BuiltReplaceOptions();
  }

  public class BuiltReplaceOptions extends BuiltCommonDurabilityOptions {

    public Duration expiry() {
      return expiry;
    }

    public Encoder encoder() {
      return encoder;
    }

    public long cas() {
      return cas;
    }

  }
}
