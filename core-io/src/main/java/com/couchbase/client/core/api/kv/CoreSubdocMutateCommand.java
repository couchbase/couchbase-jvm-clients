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

import com.couchbase.client.core.msg.kv.SubdocCommandType;
import com.couchbase.client.core.util.Bytes;
import reactor.util.annotation.Nullable;

import static com.couchbase.client.core.logging.RedactableArgument.redactUser;
import static java.nio.charset.StandardCharsets.UTF_8;

public class CoreSubdocMutateCommand {
  private final SubdocCommandType type;
  private final String path;
  private final byte[] fragment;
  private final boolean createParent;
  private final boolean xattr;
  private final boolean expandMacro;

  public CoreSubdocMutateCommand(
      SubdocCommandType type,
      String path,
      @Nullable byte[] fragment,
      boolean createParent,
      boolean xattr,
      boolean expandMacro
  ) {
    this.type = type;
    this.path = path;
    this.fragment = fragment == null ? Bytes.EMPTY_BYTE_ARRAY : fragment;
    this.createParent = createParent;
    this.xattr = xattr;
    this.expandMacro = expandMacro;
  }

  public SubdocCommandType type() {
    return type;
  }

  public String path() {
    return path;
  }

  public byte[] fragment() {
    return fragment;
  }

  public boolean createParent() {
    return createParent;
  }

  public boolean xattr() {
    return xattr;
  }

  public boolean expandMacro() {
    return expandMacro;
  }

  @Override
  public String toString() {
    return "CoreSubdocMutateCommand{" +
        "type=" + type +
        ", path='" + path + '\'' +
        ", fragment=" + redactUser(new String(fragment, UTF_8)) +
        ", createParent=" + createParent +
        ", xattr=" + xattr +
        ", expandMacro=" + expandMacro +
        '}';
  }
}
