/*
 * Copyright 2023 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.api.query;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.shared.CoreMutationState;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ArrayNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.transaction.config.CoreSingleQueryTransactionOptions;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;

/**
 * Transactions does some rather complex things with CoreQueryOptions.  It needs to set its own options in addition to those
 * set by the user (or a higher layer).  And sometimes wants to merge those options with the higher options, and sometimes
 * override the higher options entirely.  Hence this rather complex ParameterPassthrough system.
 * <p>
 * If CoreQueryOptions could be easily cloned-with-changes, ala Scala case class's copy() method, that would be preferable.
 */
@Stability.Internal
public class CoreQueryOptionsTransactions implements CoreQueryOptions {
  public enum ParameterPassthrough {
    /**
     * For primitives:
     * If a parameter has been set in the shadow then return that, otherwise return the original's version.
     * <p>
     * For non-primitives that are mergable (such as Lists and Maps):
     * Merge this parameter together with the original's.
     * For anything that's set in both, the shadow takes precedence.
     */
    DEFAULT,

    /**
     * Ignore the original parameter and always return the shadowed.
     */
    ALWAYS_SHADOWED
  }

  public enum QueryOptionsParameter {
    RAW,
    METRICS,
    AS_TRANSACTION_OPTIONS
  }

  /**
   * The original query options passed in externally.  We don't want to modify this (TXNJ-437).
   * <p>
   * Can be null where the transactions code has created this.
   * <p>
   * The final result of all getters are the original settings, merged with any settings set in `this`.  The latter take precedence if they clash.
   */
  private final @Nullable CoreQueryOptions original;

  // Only the options used by transactions are exposed - more can be added if needed.  (And this class can be converted into a more generic class not specific to transactions
  // if needed).
  private Map<String, JsonNode> raw;
  private Boolean metrics;
  private CoreSingleQueryTransactionOptions asTransactionOptions;

  private Map<QueryOptionsParameter, ParameterPassthrough> passthroughSettings;

  public CoreQueryOptionsTransactions() {
    this.original = null;
  }

  public CoreQueryOptionsTransactions(@Nullable CoreQueryOptions original) {
    this.original = original;
  }

  public CoreQueryOptionsTransactions raw(String key, JsonNode value) {
    notNullOrEmpty(key, "Key");
    if (raw == null) {
      raw = new HashMap<>();
    }
    raw.put(key, value);
    return this;
  }

  public CoreQueryOptionsTransactions metrics(boolean metrics) {
    this.metrics = metrics;
    return this;
  }

  public CoreQueryOptionsTransactions set(QueryOptionsParameter param, ParameterPassthrough passthrough) {
    if (passthroughSettings == null) {
      passthroughSettings = new HashMap<>();
    }
    this.passthroughSettings.put(param, passthrough);
    return this;
  }

  public CoreQueryOptionsTransactions put(String key, JsonNode value) {
    return raw(key, value);
  }

  @Override
  public boolean adhoc() {
    return original == null ? true : original.adhoc();
  }

  @Override
  public String clientContextId() {
    return original == null ? null : original.clientContextId();
  }

  @Override
  public CoreMutationState consistentWith() {
    return original == null ? null : original.consistentWith();
  }

  @Override
  public Integer maxParallelism() {
    return original == null ? null : original.maxParallelism();
  }

  private ParameterPassthrough passthroughFor(QueryOptionsParameter param) {
    if (passthroughSettings == null) {
      return ParameterPassthrough.DEFAULT;
    }
    ParameterPassthrough ret = passthroughSettings.get(param);
    return ret == null ? ParameterPassthrough.DEFAULT : ret;
  }

  @Override
  public boolean metrics() {
    switch (passthroughFor(QueryOptionsParameter.METRICS)) {
      case ALWAYS_SHADOWED:
        return metrics;
      default:
        return metrics != null ? metrics : original != null && original.metrics();
    }
  }

  @Override
  public ObjectNode namedParameters() {
    return original == null ? null : original.namedParameters();
  }

  @Override
  public Integer pipelineBatch() {
    return original == null ? null : original.pipelineBatch();
  }

  @Override
  public Integer pipelineCap() {
    return original == null ? null : original.pipelineCap();
  }

  @Override
  public ArrayNode positionalParameters() {
    return original == null ? null : original.positionalParameters();
  }

  @Override
  public CoreQueryProfile profile() {
    return original == null ? CoreQueryProfile.OFF : original.profile();
  }

  @Override
  public JsonNode raw() {
    switch (passthroughFor(QueryOptionsParameter.RAW)) {
      case ALWAYS_SHADOWED: {
        if (raw == null) {
          return null;
        }

        ObjectNode out = Mapper.createObjectNode();
        raw.forEach(out::set);
        return out;
      }

      default: {
        if (original != null && original.raw() != null) {
          ObjectNode out = Mapper.createObjectNode();
          JsonNode origRaw = original.raw();
          origRaw.fieldNames().forEachRemaining(fieldName -> {
            out.set(fieldName, origRaw.get(fieldName));
          });
          if (raw != null) {
            raw.forEach(out::set);
          }
          return out;
        }

        if (raw == null) {
          return null;
        }

        ObjectNode out = Mapper.createObjectNode();
        raw.forEach(out::set);
        return out;
      }
    }
  }

  @Override
  public boolean readonly() {
    return original == null ? false : original.readonly();
  }

  @Override
  public Duration scanWait() {
    return original == null ? null : original.scanWait();
  }

  @Override
  public Integer scanCap() {
    return original == null ? null : original.scanCap();
  }

  @Override
  public CoreQueryScanConsistency scanConsistency() {
    return original == null ? CoreQueryScanConsistency.NOT_BOUNDED : original.scanConsistency();
  }

  @Override
  public boolean flexIndex() {
    return original == null ? false : original.flexIndex();
  }

  @Override
  public Boolean preserveExpiry() {
    return original == null ? null : original.preserveExpiry();
  }

  @Override
  public CoreSingleQueryTransactionOptions asTransactionOptions() {
    switch (passthroughFor(QueryOptionsParameter.AS_TRANSACTION_OPTIONS)) {
      case ALWAYS_SHADOWED:
        return asTransactionOptions;

      default:
        if (asTransactionOptions != null) {
          // Believe we shouldn't reach here, and is much simpler than trying to merge the options...
          throw new IllegalStateException("Internal bug - should not reach here");
        }

        return original == null ? null : original.asTransactionOptions();
    }
  }

  @Override
  public Map<String, Object> clientContext() {
    return original == null ? null : original.clientContext();
  }

  @Override
  public CoreCommonOptions commonOptions() {
    return original == null ? CoreCommonOptions.DEFAULT : original.commonOptions();
  }
}
