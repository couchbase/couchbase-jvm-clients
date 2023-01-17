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

import java.util.List;
import java.util.function.Supplier;

import static com.couchbase.client.core.util.Validators.notNullOrEmpty;

@Stability.Internal
public class CoreKvParamValidators {
  private CoreKvParamValidators() {}

  public static void validateGetParams(CoreCommonOptions common, String key, List<String> projections, boolean withExpiry) {
    notNullOrEmpty(key, "Document ID");
  }

  public static void validateInsertParams(CoreCommonOptions common, String key, Supplier<CoreEncodedContent> content, CoreDurability durability, long expiry) {
    notNullOrEmpty(key, "Document ID");
  }

  public static void validateRemoveParams(CoreCommonOptions common, String key, long cas, CoreDurability durability) {
    notNullOrEmpty(key, "Document ID");
  }

}
