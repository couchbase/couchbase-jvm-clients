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
import com.couchbase.client.java.codec.Transcoder;

import java.time.Duration;

import static com.couchbase.client.core.util.Validators.notNull;

public class UpsertOptions extends CommonDurabilityOptions<UpsertOptions> {

  private Duration expiry = Duration.ZERO;
  private Transcoder transcoder;

  private UpsertOptions() { }

  public static UpsertOptions upsertOptions() {
    return new UpsertOptions();
  }

  public UpsertOptions expiry(final Duration expiry) {
    this.expiry = expiry;
    return this;
  }

  public UpsertOptions transcoder(final Transcoder transcoder) {
    notNull(transcoder, "Transcoder");
    this.transcoder = transcoder;
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

    public Transcoder transcoder() {
      return transcoder;
    }

  }
}
