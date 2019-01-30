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

public class Projections {

  private final List<SubdocGetRequest.Command> commands;

  public static Projections projections() {
    return new Projections();
  }

  private Projections() {
    commands = new ArrayList<>();
  }

  private Projections add(SubdocCommandType type, boolean xattr, String... paths) {
    for (String path : paths) {
      commands.add(new SubdocGetRequest.Command(type, path, xattr));
    }
    return this;
  }

  public Projections get(String... paths) {
    return add(SubdocCommandType.GET, false, paths);
  }

  public Projections exists(String... paths) {
    return add(SubdocCommandType.EXISTS, false, paths);
  }

  public Projections count(String... paths) {
    return add(SubdocCommandType.COUNT, false, paths);
  }

  public Projections getXAttr(String... paths) {
    return add(SubdocCommandType.GET, true, paths);
  }

  public Projections existsXAttr(String... paths) {
    return add(SubdocCommandType.EXISTS, true, paths);
  }

  public Projections countXAttr(String... paths) {
    return add(SubdocCommandType.COUNT, true, paths);
  }

  @Stability.Internal
  public List<SubdocGetRequest.Command> commands() {
    return commands;
  }


}
