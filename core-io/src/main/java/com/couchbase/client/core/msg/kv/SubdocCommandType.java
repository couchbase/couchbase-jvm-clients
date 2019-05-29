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

package com.couchbase.client.core.msg.kv;

public enum SubdocCommandType {
  // mutateIn
  SET_DOC((byte) 0x01),
  COUNTER((byte) 0xcf),
  REPLACE((byte) 0xca),
  DICT_ADD((byte) 0xc7),
  DICT_UPSERT((byte) 0xc8),
  ARRAY_PUSH_FIRST((byte) 0xcc),
  ARRAY_PUSH_LAST((byte) 0xcb),
  ARRAY_ADD_UNIQUE((byte) 0xce),
  ARRAY_INSERT((byte) 0xcd),
  DELETE((byte) 0xc9),

  // lookupIn
  GET((byte) 0xc5),
  EXISTS((byte) 0xc6),
  COUNT((byte) 0xd2),
  GET_DOC((byte) 0x00);

  private final byte opcode;

  SubdocCommandType(byte opcode) {
    this.opcode = opcode;
  }

  byte opcode() {
    return opcode;
  }
}
