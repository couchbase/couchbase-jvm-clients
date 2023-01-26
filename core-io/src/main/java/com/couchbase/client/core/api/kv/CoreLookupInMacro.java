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

/**
 * Convenience macros to retrieve document metadata using a lookupIn Sub-Document call.
 */
@Stability.Internal
public class CoreLookupInMacro {

  private CoreLookupInMacro() {
    throw new AssertionError("not instantiable");
  }

  public static final String DOCUMENT = "$document";

  public static final String EXPIRY_TIME = "$document.exptime";

  public static final String CAS = "$document.CAS";

  public static final String SEQ_NO = "$document.seqno";

  public static final String LAST_MODIFIED = "$document.last_modified";

  public static final String IS_DELETED = "$document.deleted";

  public static final String VALUE_SIZE_BYTES = "$document.value_bytes";

  public static final String REV_ID = "$document.revid";

  public static final String FLAGS = "$document.flags";

}
