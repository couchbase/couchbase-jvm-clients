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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.msg.kv.SubdocCommandType;
import com.couchbase.client.core.msg.kv.SubdocGetRequest;

public abstract class LookupInSpec {
  @Stability.Internal
  abstract public SubdocGetRequest.Command export(int originalIndex);

  public static LookupInSpecStandard get(final String path) {
    SubdocCommandType command = path.equals("") ? SubdocCommandType.GET_DOC : SubdocCommandType.GET;
    return new LookupInSpecStandard(command, path);
  }

  public static LookupInSpecStandard exists(final String path) {
    return new LookupInSpecStandard(SubdocCommandType.EXISTS, path);
  }

  public static LookupInSpecStandard count(final String path) {
    return new LookupInSpecStandard(SubdocCommandType.COUNT, path);
  }

}

