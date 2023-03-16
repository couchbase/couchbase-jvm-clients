/*
 * Copyright (c) 2023 Couchbase, Inc.
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
package com.couchbase.client.core.api.kv;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.error.context.ReducedKeyValueErrorContext;
import com.couchbase.client.core.io.CollectionIdentifier;

import java.time.Duration;
import java.util.List;
import java.util.function.Supplier;

import static com.couchbase.client.core.api.kv.CoreStoreSemantics.REPLACE;
import static com.couchbase.client.core.api.kv.CoreStoreSemantics.REVIVE;
import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;

@Stability.Internal
public class CoreKvParamValidators {
  private CoreKvParamValidators() {}

  public static void validateGetParams(CoreCommonOptions common, String key, List<String> projections, boolean withExpiry) {
    validateCommonOptions(common, key);
  }

  public static void validateInsertParams(CoreCommonOptions common, String key, Supplier<CoreEncodedContent> content, CoreDurability durability, CoreExpiry expiry) {
    validateCommonOptions(common, key);
    notNull(expiry, "expiry");
  }

  public static void validateUpsertParams(CoreCommonOptions common, String key, Supplier<CoreEncodedContent> content, CoreDurability durability, CoreExpiry expiry, boolean preserveExpiry) {
    validateCommonOptions(common, key);
    notNull(expiry, "expiry");
  }

  public static void validateReplaceParams(CoreCommonOptions common, String key, Supplier<CoreEncodedContent> content, long cas, CoreDurability durability, CoreExpiry expiry, boolean preserveExpiry) {
    validateCommonOptions(common, key);
    notNull(expiry, "expiry");
  }

  public static void validateRemoveParams(CoreCommonOptions common, String key, long cas, CoreDurability durability) {
    validateCommonOptions(common, key);
  }

  public static void validateExistsParams(CoreCommonOptions common, String key) {
    validateCommonOptions(common, key);
  }

  public static void validateGetAndLockParams(CoreCommonOptions common, String key, Duration lockTime) {
    validateCommonOptions(common, key);
    notNull(lockTime, "lockTime");
  }

  public static void validateGetAndTouchParams(CoreCommonOptions common, String key, CoreExpiry expiry) {
    validateCommonOptions(common, key);
    notNull(expiry, "expiry");
  }

  public static void validateTouchParams(CoreCommonOptions common, String key, CoreExpiry expiry) {
    validateCommonOptions(common, key);
    notNull(expiry, "expiry");
  }

  public static void validateUnlockParams(CoreCommonOptions common, String key, long cas, CollectionIdentifier collectionIdentifier) {
    validateCommonOptions(common, key);
    if (cas == 0) {
      throw new InvalidArgumentException("Unlock CAS must not be 0", null, ReducedKeyValueErrorContext.create(key, collectionIdentifier));
    }
  }

  public static void validateGetAllReplicasParams(CoreCommonOptions common, String key) {
    validateCommonOptions(common, key);
  }

  public static void validateGetAnyReplicaParams(CoreCommonOptions common, String key) {
    validateCommonOptions(common, key);
  }

  public static void validateSubdocMutateParams(CoreCommonOptions common, String key, CoreStoreSemantics storeSemantics, long cas) {
    validateCommonOptions(common, key);
    if (cas != 0 && (storeSemantics != REPLACE && storeSemantics != REVIVE)) {
      // Not mentioning REVIVE in the user-facing error message, since it's internal API.
      throw InvalidArgumentException.fromMessage("A non-zero CAS value requires \"replace\" store semantics.");
    }
  }

  private static void validateCommonOptions(CoreCommonOptions common, String key) {
    notNull(common, "Common Options");
    notNullOrEmpty(key, "Document ID");
  }

}
