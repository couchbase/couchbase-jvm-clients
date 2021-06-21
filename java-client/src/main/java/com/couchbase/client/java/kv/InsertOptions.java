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
import java.time.Instant;

import static com.couchbase.client.core.util.Validators.notNull;

public class InsertOptions extends CommonDurabilityOptions<InsertOptions> {

  private Expiry expiry = Expiry.none();
  private Transcoder transcoder;

  private InsertOptions() { }

  public static InsertOptions insertOptions() {
    return new InsertOptions();
  }

  /**
   * Sets the expiry for the document. By default the document will never expire.
   * <p>
   * The duration must be less than 50 years. For expiry further in the
   * future, use {@link #expiry(Instant)}.
   *
   * @param expiry the duration after which the document will expire (zero duration means never expire).
   * @return this options class for chaining purposes.
   */
  public InsertOptions expiry(final Duration expiry) {
    this.expiry = Expiry.relative(expiry);
    return this;
  }

  /**
   * Sets the expiry for the document. By default the document will never expire.
   *
   * @param expiry the point in time when the document will expire (epoch second zero means never expire).
   * @return this options class for chaining purposes.
   */
  public InsertOptions expiry(final Instant expiry) {
    this.expiry = Expiry.absolute(expiry);
    return this;
  }

  /**
   * Allows to specify a custom transcoder that is used to encode the content of the request.
   *
   * @param transcoder the custom transcoder that should be used for encoding.
   * @return the {@link InsertOptions} to allow method chaining.
   */
  public InsertOptions transcoder(final Transcoder transcoder) {
    this.transcoder = notNull(transcoder, "Transcoder");
    return this;
  }

  @Stability.Internal
  public Built build() {
    return new Built();
  }

  public class Built extends BuiltCommonDurabilityOptions {

    Built() { }

    public Expiry expiry() {
      return expiry;
    }

    public Transcoder transcoder() {
      return transcoder;
    }

  }
}
