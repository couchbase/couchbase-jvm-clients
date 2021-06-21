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
import com.couchbase.client.core.error.CasMismatchException;
import com.couchbase.client.core.error.InvalidArgumentException;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

/**
 * Modifies properties of the increment operation.
 */
public class IncrementOptions extends CommonDurabilityOptions<IncrementOptions> {

  /**
   * Stores the delta for the operation.
   */
  private long delta = 1;

  /**
   * If present, holds the initial value.
   */
  private Optional<Long> initial = Optional.empty();

  /**
   * If set, holds the expiration for this operation.
   */
  private Expiry expiry = Expiry.none();

  /**
   * Creates a new {@link IncrementOptions}.
   *
   * @return the created options.
   */
  public static IncrementOptions incrementOptions() {
    return new IncrementOptions();
  }

  private IncrementOptions() { }

  /**
   * The amount of which the document value should be incremented.
   *
   * @param delta the amount to increment.
   * @return this options class for chaining purposes.
   */
  public IncrementOptions delta(long delta) {
    if (delta < 0) {
      throw InvalidArgumentException.fromMessage("The delta cannot be less than 0");
    }
    this.delta = delta;
    return this;
  }

  /**
   * The initial value that should be used if the document has not been created yet.
   *
   * @param initial the initial value to use.
   * @return this options class for chaining purposes.
   */
  public IncrementOptions initial(long initial) {
    this.initial = Optional.of(initial);
    return this;
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
  public IncrementOptions expiry(final Duration expiry) {
    this.expiry = Expiry.relative(expiry);
    return this;
  }

  /**
   * Sets the expiry for the document. By default the document will never expire.
   *
   * @param expiry the point in time when the document will expire (epoch second zero means never expire).
   * @return this options class for chaining purposes.
   */
  public IncrementOptions expiry(final Instant expiry) {
    this.expiry = Expiry.absolute(expiry);
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

    public Optional<Long> initial() {
      return initial;
    }

    public long delta() {
      return delta;
    }

  }

}
