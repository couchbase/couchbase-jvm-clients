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

public class LookupInSpec {

  private final String path;
  private final SubdocCommandType type;
  private boolean xattr = false;

  private LookupInSpec(SubdocCommandType type, String path) {
    this.path = path;
    this.type = type;
  }

  public static LookupInSpec get(final String path) {
    return new LookupInSpec(SubdocCommandType.GET, path);
  }

  public static LookupInSpec exists(final String path) {
    return new LookupInSpec(SubdocCommandType.EXISTS, path);
  }

  public static LookupInSpec count(final String path) {
    return new LookupInSpec(SubdocCommandType.COUNT, path);
  }

  public LookupInSpec xattr() {
    xattr = true;
    return this;
  }

  @Stability.Internal
  public SubdocGetRequest.Command export() {
    return new SubdocGetRequest.Command(type, path, xattr);
  }

}
