/*
 * Copyright (c) 2019 Couchbase, Inc.
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

@Stability.Volatile
public enum MutateInMacro {
  CAS("${Mutation.CAS}"),

  SEQ_NO("${Mutation.seqno}"),

  VALUE_CRC_32C("${Mutation.value_crc32c");

  private final String value;

  MutateInMacro(String value) {
    this.value = value;
  }

  String value() {
    return value;
  }
}
