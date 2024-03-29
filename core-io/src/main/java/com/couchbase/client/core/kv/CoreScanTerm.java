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
package com.couchbase.client.core.kv;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.util.CbStrings;

import static com.couchbase.client.core.util.Validators.notNullOrEmpty;

/**
 * A single {@link CoreScanTerm} identifying either the point to scan from or to scan to.
 */
@Stability.Internal
public class CoreScanTerm {

  public static final CoreScanTerm MIN = new CoreScanTerm(CbStrings.MIN_CODE_POINT_AS_STRING, false);
  public static final CoreScanTerm MAX = new CoreScanTerm(CbStrings.MAX_CODE_POINT_AS_STRING, true);

  private final String id;
  private final boolean exclusive;

  public CoreScanTerm(String id, boolean exclusive) {
    this.id = notNullOrEmpty(id, "ScanTerm ID");
    this.exclusive = exclusive;
  }

  public String id() {
    return id;
  }

  public boolean exclusive() {
    return exclusive;
  }
}

