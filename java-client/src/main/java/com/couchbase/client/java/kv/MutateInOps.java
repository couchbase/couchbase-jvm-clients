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
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.core.msg.kv.SubdocCommandType;
import com.couchbase.client.core.msg.kv.SubdocMutateRequest;
import com.couchbase.client.java.codec.DefaultEncoder;
import com.couchbase.client.java.codec.Encoder;

import java.util.ArrayList;
import java.util.List;

public class MutateInOps {

  private final List<SubdocMutateRequest.Command> commands;

  private static final Encoder ENCODER = new DefaultEncoder();

  public static MutateInOps mutateInOps() {
    return new MutateInOps();
  }

  private MutateInOps() {
    commands = new ArrayList<>();
  }

  public <T> MutateInOps replace(final boolean xattr, final String path, final T fragment) {
    return replace(xattr, path, fragment, false);
  }

  public <T> MutateInOps replace(final String path, final T fragment, final boolean createParent) {
    return replace(false, path, fragment, createParent);
  }

  public <T> MutateInOps replace(final String path, final T fragment) {
    return replace(false, path, fragment, false);
  }

  public <T> MutateInOps replace(final boolean xattr, final String path, final T fragment,
                                 final boolean createParent) {
    commands.add(new SubdocMutateRequest.Command(
      SubdocCommandType.REPLACE,
      path,
      ENCODER.encode(fragment).content(),
      createParent,
      xattr
    ));
    return this;
  }

  public <T> MutateInOps insert(final boolean xattr, final String path, final T fragment) {
    return insert(xattr, path, fragment, false);
  }

  public <T> MutateInOps insert(final String path, final T fragment, final boolean createParent) {
    return insert(false, path, fragment, createParent);
  }


  public <T> MutateInOps insert(final String path, final T fragment) {
    return insert(false, path, fragment, false);
  }

  public <T> MutateInOps insert(final boolean xattr, final String path, final T fragment,
                                 final boolean createParent) {
    commands.add(new SubdocMutateRequest.Command(
      SubdocCommandType.DICT_ADD,
      path,
      ENCODER.encode(fragment).content(),
      createParent,
      xattr
    ));
    return this;
  }


  @Stability.Internal
  public List<SubdocMutateRequest.Command> commands() {
    return commands;
  }

}

