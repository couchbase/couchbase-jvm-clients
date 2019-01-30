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

import java.util.ArrayList;
import java.util.List;

public class LookupInOps {

  private final List<SubdocGetRequest.Command> commands;

  public static LookupInOps lookupInOps() {
    return new LookupInOps();
  }

  private LookupInOps() {
    commands = new ArrayList<>();
  }

  public LookupInOps get(final String... paths) {
    return get(false, paths);
  }

  public LookupInOps get(boolean xattr, final String... paths) {
    for (String path : paths) {
      commands.add(
        new SubdocGetRequest.Command(SubdocCommandType.GET, path, xattr)
      );
    }
    return this;
  }

  public LookupInOps exists(final String... paths) {
    return exists(false, paths);
  }

  public LookupInOps exists(boolean xattr, final String... paths) {
    for (String path : paths) {
      commands.add(
        new SubdocGetRequest.Command(SubdocCommandType.EXISTS, path, xattr)
      );
    }
    return this;
  }

  public LookupInOps count(final String... paths) {
    return count(false, paths);
  }

  public LookupInOps count(boolean xattr, final String... paths) {
    for (String path : paths) {
      commands.add(
        new SubdocGetRequest.Command(SubdocCommandType.COUNT, path, xattr)
      );
    }
    return this;
  }

  @Stability.Internal
  public List<SubdocGetRequest.Command> commands() {
    return commands;
  }
}
