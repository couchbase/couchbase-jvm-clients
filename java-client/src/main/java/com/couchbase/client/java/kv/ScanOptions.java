/*
 * Copyright (c) 2022 Couchbase, Inc.
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

import static com.couchbase.client.core.kv.RangeScanOrchestrator.RANGE_SCAN_DEFAULT_BATCH_BYTE_LIMIT;
import static com.couchbase.client.core.kv.RangeScanOrchestrator.RANGE_SCAN_DEFAULT_BATCH_ITEM_LIMIT;
import static com.couchbase.client.core.util.Validators.notNull;

import java.time.Duration;
import java.util.Optional;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.shared.CoreMutationState;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.kv.CoreScanOptions;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.java.CommonOptions;
import com.couchbase.client.java.codec.Transcoder;

/**
 * Allows to customize the various range and sampling scan options.
 */
@Stability.Volatile
public class ScanOptions extends CommonOptions<ScanOptions> {

  public static ScanOptions scanOptions() {
    return new ScanOptions();
  }

  private boolean idsOnly = false;

  /**
   * The default batch item limit is 50.
   */
  private int batchItemLimit = RANGE_SCAN_DEFAULT_BATCH_ITEM_LIMIT;

  /**
   * The default batch byte limit is 15k.
   */
  private int batchByteLimit = RANGE_SCAN_DEFAULT_BATCH_BYTE_LIMIT;

  private Optional<MutationState> consistentWith = Optional.empty();

  /**
   * Holds the transcoder used for decoding.
   */
  private Transcoder transcoder;

  private ScanOptions() {

  }

  /**
   * If set to true, the content of the document is not included in the results.
   *
   * @param idsOnly True means document content and metadata are excluded from result; default is false.
   * @return the {@link ScanOptions} to allow method chaining.
   */
  public ScanOptions idsOnly(final boolean idsOnly) {
    this.idsOnly = idsOnly;
    return this;
  }

  /**
   * Allows to specify a custom transcoder that is used to decode the content of the result.
   *
   * @param transcoder the custom transcoder that should be used for decoding.
   * @return the {@link ScanOptions} to allow method chaining.
   */
  public ScanOptions transcoder(final Transcoder transcoder) {
    this.transcoder = notNull(transcoder, "Transcoder");
    return this;
  }

  public ScanOptions consistentWith(final MutationState mutationState) {
    this.consistentWith = Optional.ofNullable(mutationState);
    return this;
  }

  /**
   * Allows to limit the maximum amount of bytes that are sent from the server to the client on each partition batch.
   * <p>
   * This option:
   * <ul>
   *   <li>will be applied to each partition stream individually, not to the operation as a whole.</li>
   *   <li>only acts as a target the server aims to satisfy, not a hard limit.</li>
   * </ul>
   * The SDK will maintain one stream for each partition and aggregate the results for the user. The limit set here
   * will affect each individual partition and allows to optimize performance with larger batches. Keep in mind that
   * a larger value is not always better since sockets will be occupied for a longer time and might have a negative
   * latency impact on other operations that are waiting.
   * <p>
   * Also, the server will always send the document body, even if the configured byte limit is smaller. So as an example
   * if a batchByteLimit of 1MB (1_000_000) is set and the document body size is 5MB it will still be sent to the client as a whole
   * and not cut off in between.
   * <p>
   * If both this option and {@link #batchItemLimit(int)} are set, both will be sent to the server
   * and the smaller one will trigger first.
   *
   * @param batchByteLimit the byte limit to set per stream, defaults to 15000 (15k).
   * @return the {@link ScanOptions} to allow method chaining.
   */
  public ScanOptions batchByteLimit(final int batchByteLimit) {
    if (batchByteLimit < 0) {
      throw InvalidArgumentException.fromMessage("The batchByteLimit must not be smaller than 0");
    }
    this.batchByteLimit = batchByteLimit;
    return this;
  }

  /**
   * Allows to limit the maximum amount of documents that are sent from the server to the client on each partition batch.
   * <p>
   * This option:
   * <ul>
   *   <li>will be applied to each partition stream individually, not to the operation as a whole.</li>
   * </ul>
   * The SDK will maintain one stream for each partition and aggregate the results for the user. The limit set here
   * will affect each individual partition and allows to optimize performance with larger batches. Keep in mind that
   * a larger value is not always better since sockets will be occupied for a longer time and might have a negative
   * latency impact on other operations that are waiting.
   * <p>
   * If both this option and {@link #batchByteLimit(int)} are set, both will be sent to the server and the smaller
   * one will trigger first.
   *
   * @param batchItemLimit the item limit to set per stream, defaults to 50.
   * @return the {@link ScanOptions} to allow method chaining.
   */
  public ScanOptions batchItemLimit(final int batchItemLimit) {
    if (batchItemLimit < 0) {
      throw InvalidArgumentException.fromMessage("The batchItemLimit must not be smaller than 0");
    }
    this.batchItemLimit = batchItemLimit;
    return this;
  }

  @Stability.Internal
  public Built build() {
    return new Built();
  }

  @Stability.Internal
  public class Built extends BuiltCommonOptions implements CoreScanOptions {

    @Override
    public CoreCommonOptions commonOptions(){
      return this;
    }

    @Override
    public boolean idsOnly() {
      return idsOnly;
    }

    public Transcoder transcoder() {
      return transcoder;
    }

    @Override
    public CoreMutationState consistentWith() {
      return consistentWith.map(CoreMutationState::new).orElse(null);
    }

    @Override
    public int batchItemLimit() {
      return batchItemLimit;
    }

    @Override
    public int batchByteLimit() {
      return batchByteLimit;
    }

  }

}
