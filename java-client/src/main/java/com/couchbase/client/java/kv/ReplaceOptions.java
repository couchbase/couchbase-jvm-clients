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
import com.couchbase.client.java.codec.Transcoder;

import java.time.Duration;
import java.time.Instant;

import static com.couchbase.client.core.util.Validators.notNull;

public class ReplaceOptions extends CommonDurabilityOptions<ReplaceOptions> {

  private Expiry expiry = Expiry.none();
  private Transcoder transcoder;
  private long cas;

  private ReplaceOptions() { }

  public static ReplaceOptions replaceOptions() {
    return new ReplaceOptions();
  }

  /**
   * Sets the expiry time for the document as a relative duration.
   * <p>
   * IMPORTANT: we recommend using a relative duration only if the provided value is less than 30 days. The reason
   * is that the server will assume any value larger than that to be an absolute unix timestamp. The SDK tries its
   * best to coerce it into sane values, but to avoid any unexpected behavior please stick to the less than 30 days
   * as a relative duration. For every expiry > 30 days, please provide it as an absolute instant through the
   * {@link #expiry(Instant)} overload.
   *
   * @param expiry the expiry time as a relative duration.
   * @return this {@link ReplaceOptions} for chaining purposes.
   */
  public ReplaceOptions expiry(final Duration expiry) {
    this.expiry = Expiry.relative(expiry);
    return this;
  }

  /**
   * Sets the expiry time for the document as a absolute duration.
   * <p>
   * Note that the absolute instant will be converted into a unix timestamp in seconds before sending it over the
   * wire to the server. If you need to provide a relative duration you can use {@link #expiry(Duration)} but please
   * see its javadoc for common pitfalls and edge cases. If in doubt, please provide it as an absolute instant on this
   * overload.
   *
   * @param expiry the expiry time as an absolute instant.
   * @return this {@link ReplaceOptions} for chaining purposes.
   */
  @Stability.Uncommitted
  public ReplaceOptions expiry(final Instant expiry) {
    this.expiry = Expiry.absolute(expiry);
    return this;
  }

  /**
   * Allows to specify a custom transcoder that is used to encode the content of the request.
   *
   * @param transcoder the custom transcoder that should be used for encoding.
   * @return the {@link ReplaceOptions} to allow method chaining.
   */
  public ReplaceOptions transcoder(final Transcoder transcoder) {
    this.transcoder = notNull(transcoder, "Transcoder");
    return this;
  }

  /**
   * Specifies a CAS value that will be taken into account on the server side for optimistic concurrency.
   * <p>
   * The CAS value is an opaque identifier which is associated with a specific state of the document on the server. The
   * CAS value is received on read operations (or after mutations) and can be used during a subsequent mutation to
   * make sure that the document has not been modified in the meantime.
   * <p>
   * If document on the server has been modified in the meantime the SDK will raise a {@link CasMismatchException}. In
   * this case the caller is expected to re-do the whole "fetch-modify-update" cycle again. Please refer to the
   * SDK documentation for more information on CAS mismatches and subsequent retries.
   *
   * @param cas the opaque CAS identifier to use for this operation.
   * @return the {@link ReplaceOptions} for chaining purposes.
   */
  public ReplaceOptions cas(long cas) {
    this.cas = cas;
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

    public long cas() {
      return cas;
    }

  }
}
