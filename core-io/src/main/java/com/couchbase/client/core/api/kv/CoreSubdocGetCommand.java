/*
 * Copyright 2023 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.api.kv;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.msg.kv.SubdocCommandType;

import static com.couchbase.client.core.logging.RedactableArgument.redactUser;
import static java.util.Objects.requireNonNull;

@Stability.Internal
public class CoreSubdocGetCommand {
  private final SubdocCommandType type;
  private final String path;
  private final boolean xattr;

  public CoreSubdocGetCommand(SubdocCommandType type, String path, boolean xattr) {
    this.type = requireNonNull(type);
    this.path = requireNonNull(path);
    this.xattr = xattr;
  }

  public SubdocCommandType type() {
    return type;
  }

  public String path() {
    return path;
  }

  public boolean xattr() {
    return xattr;
  }

  @Override
  public String toString() {
    return "CoreSubdocGetCommand{" +
        "type=" + type +
        ", path='" + redactUser(path) + '\'' +
        ", xattr=" + xattr +
        '}';
  }
}

