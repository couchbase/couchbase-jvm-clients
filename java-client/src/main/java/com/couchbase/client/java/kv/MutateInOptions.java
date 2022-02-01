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

import com.couchbase.client.core.annotation.SinceCouchbase;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.error.CasMismatchException;
import com.couchbase.client.java.codec.JsonSerializer;

import java.time.Duration;
import java.time.Instant;

import static com.couchbase.client.core.util.Validators.notNull;

public class MutateInOptions extends CommonDurabilityOptions<MutateInOptions> {

  private Expiry expiry = Expiry.none();
  private boolean preserveExpiry;
  private long cas = 0;
  private StoreSemantics storeSemantics = StoreSemantics.REPLACE;
  private JsonSerializer serializer = null;
  private boolean accessDeleted = false;
  private boolean createAsDeleted = false;

  public static MutateInOptions mutateInOptions() {
    return new MutateInOptions();
  }

  private MutateInOptions() {
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
  public MutateInOptions expiry(final Duration expiry) {
    this.expiry = Expiry.relative(expiry);
    return this;
  }

  /**
   * Sets the expiry for the document. By default the document will never expire.
   *
   * @param expiry the point in time when the document will expire (epoch second zero means never expire).
   * @return this options class for chaining purposes.
   */
  public MutateInOptions expiry(final Instant expiry) {
    this.expiry = Expiry.absolute(expiry);
    return this;
  }

  /**
   * Specifies whether an existing document's expiry should be preserved.
   * Defaults to false.
   * <p>
   * If true, and the document exists, its expiry will not be modified.
   * Otherwise the document's expiry is determined by
   * {@link #expiry(Duration)} or {@link #expiry(Instant)}.
   * <p>
   * Requires Couchbase Server 7.0 or later.
   *
   * @param preserveExpiry true to preserve expiry, false to set new expiry
   * @return this options class for chaining purposes.
   */
  @Stability.Uncommitted
  @SinceCouchbase("7.0")
  public MutateInOptions preserveExpiry(boolean preserveExpiry) {
    this.preserveExpiry = preserveExpiry;
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
   * @return the {@link MutateInOptions} for chaining purposes.
   */
  public MutateInOptions cas(long cas) {
    this.cas = cas;
    return this;
  }

  /**
   * Customizes the serializer that is used to encoded the contents of this request.
   *
   * @param serializer the serializer used for encoding.
   * @return this {@link MutateInOptions} for chaining purposes.
   */
  public MutateInOptions serializer(final JsonSerializer serializer) {
    this.serializer = notNull(serializer, "Serializer");
    return this;
  }

  /**
   * Changes the storing semantics of the outer/enclosing document.
   * <p>
   * While each individual {@link MutateInSpec} describes the semantics of the respective sub-document section, the
   * {@link StoreSemantics} are applied to the outer enclosing document as a whole. You can think of using the same
   * verb for a {@link StoreSemantics StoreSemantic} aligns with the corresponding full document. So for example a
   * {@link StoreSemantics#INSERT} works semantically similar to a
   * {@link com.couchbase.client.java.Collection#insert(String, Object)} and will fail if the document as a whole
   * already exists.
   *
   * @param storeSemantics the store semantics to apply to the document.
   * @return this {@link MutateInOptions} for chaining purposes.
   */
  public MutateInOptions storeSemantics(final StoreSemantics storeSemantics) {
    this.storeSemantics = notNull(storeSemantics, "StoreSemantics");
    return this;
  }

  /**
   * For internal use only: allows access to deleted documents that are in 'tombstone' form.
   */
  @Stability.Internal
  public MutateInOptions accessDeleted(final boolean accessDeleted) {
    this.accessDeleted = accessDeleted;
    return this;
  }

  /**
   * For internal use only: allows creating documents in 'tombstone' form.
   */
  @Stability.Internal
  public MutateInOptions createAsDeleted(final boolean createAsDeleted) {
    this.createAsDeleted = createAsDeleted;
    return this;
  }

  @Stability.Internal
  public Built build() {
    return new Built();
  }

  public class Built extends BuiltCommonDurabilityOptions {

    Built() {
    }

    public Expiry expiry() {
      return expiry;
    }

    public boolean preserveExpiry() {
      return preserveExpiry;
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

    public boolean createAsDeleted() {
      return createAsDeleted;
    }
  }
}
