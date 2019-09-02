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
import com.couchbase.client.java.codec.DataFormat;
import com.couchbase.client.java.codec.DefaultTranscoder;
import com.couchbase.client.java.codec.Transcoder;

import java.time.Duration;

import static com.couchbase.client.core.util.Validators.notNull;

public class ReplaceOptions extends CommonDurabilityOptions<ReplaceOptions> {

  private Duration expiry = Duration.ZERO;
  private DataFormat dataFormat = DataFormat.DEFAULT_DATA_FORMAT;
  private Transcoder transcoder = DefaultTranscoder.INSTANCE;
  private long cas;

  private ReplaceOptions() { }

  public static ReplaceOptions replaceOptions() {
    return new ReplaceOptions();
  }

  public ReplaceOptions expiry(final Duration expiry) {
    this.expiry = expiry;
    return this;
  }

  public ReplaceOptions dataFormat(final DataFormat dataFormat) {
    notNull(dataFormat, "DataFormat");
    this.dataFormat = dataFormat;
    return this;
  }

  public ReplaceOptions transcoder(final Transcoder transcoder) {
    notNull(transcoder, "Transcoder");
    this.transcoder = transcoder;
    return this;
  }

  public ReplaceOptions cas(long cas) {
    this.cas = cas;
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

    public DataFormat dataFormat() {
      return dataFormat;
    }

    public Transcoder transcoder() {
      return transcoder;
    }

    public long cas() {
      return cas;
    }

  }
}
