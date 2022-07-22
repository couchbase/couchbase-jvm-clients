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

package com.couchbase.client.core.kv;

import com.couchbase.client.core.util.Bytes;

public class LastCoreRangeScanItem extends CoreRangeScanItem {

  public static final LastCoreRangeScanItem INSTANCE = new LastCoreRangeScanItem();

  private LastCoreRangeScanItem() {
    super(0, null, 0, 0, Bytes.EMPTY_BYTE_ARRAY, Bytes.EMPTY_BYTE_ARRAY);
  }

  @Override
  public String toString() {
    return "LastCoreRangeScanItem{}";
  }

}
