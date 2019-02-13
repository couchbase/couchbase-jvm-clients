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

// todo: these are MutateInSpec in the spec
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
    return replace(xattr, path, fragment, false, false);
  }

  public <T> MutateInOps replace(final String path, final T fragment, final boolean createParent) {
    return replace(false, path, fragment, createParent, false);
  }

  public <T> MutateInOps replace(final String path, final T fragment) {
    return replace(false, path, fragment, false, false);
  }

  public <T> MutateInOps replace(final boolean xattr, final String path, final T fragment,
                                 final boolean createParent, final boolean expandMacro) {
    commands.add(new SubdocMutateRequest.Command(
      SubdocCommandType.REPLACE,
      path,
      ENCODER.encode(fragment).content(),
      createParent,
      xattr,
      expandMacro
    ));
    return this;
  }

  public <T> MutateInOps insert(final boolean xattr, final String path, final T fragment) {
    return insert(xattr, path, fragment, false, false);
  }

  public <T> MutateInOps insert(final String path, final T fragment, final boolean createParent) {
    return insert(false, path, fragment, createParent, false);
  }


  public <T> MutateInOps insert(final String path, final T fragment) {
    return insert(false, path, fragment, false, false);
  }

  public <T> MutateInOps insert(final boolean xattr, final String path, final T fragment,
                                 final boolean createParent, final boolean expandMacro) {
    commands.add(new SubdocMutateRequest.Command(
      SubdocCommandType.DICT_ADD,
      path,
      ENCODER.encode(fragment).content(),
      createParent,
      xattr,
      expandMacro
    ));
    return this;
  }

  public <T> MutateInOps remove(final String path) {
    return remove(false, path);
  }

  public <T> MutateInOps remove(final boolean xattr, final String path) {
    commands.add(new SubdocMutateRequest.Command(
            SubdocCommandType.DELETE,
            path,
            new byte[] {},
            false,
            xattr,
            false
    ));
    return this;
  }


  public <T> MutateInOps upsert(final boolean xattr, final String path, final T fragment) {
    return upsert(xattr, path, fragment, false, false);
  }

  public <T> MutateInOps upsert(final String path, final T fragment, final boolean createParent) {
    return upsert(false, path, fragment, createParent, false);
  }


  public <T> MutateInOps upsert(final String path, final T fragment) {
    return upsert(false, path, fragment, false, false);
  }

  public <T> MutateInOps upsert(final boolean xattr, final String path, final T fragment,
                                final boolean createParent, final boolean expandMacro) {
    commands.add(new SubdocMutateRequest.Command(
            SubdocCommandType.DICT_UPSERT,
            path,
            ENCODER.encode(fragment).content(),
            createParent,
            xattr,
            expandMacro
    ));
    return this;
  }

  public <T> MutateInOps arrayAppend(final boolean xattr, final String path, final T fragment) {
    return arrayAppend(xattr, path, fragment, false, false);
  }

  public <T> MutateInOps arrayAppend(final String path, final T fragment, final boolean createParent) {
    return arrayAppend(false, path, fragment, createParent, false);
  }


  public <T> MutateInOps arrayAppend(final String path, final T fragment) {
    return arrayAppend(false, path, fragment, false, false);
  }

  public <T> MutateInOps arrayAppend(final boolean xattr, final String path, final T fragment,
                                final boolean createParent, final boolean expandMacro) {
    commands.add(new SubdocMutateRequest.Command(
            SubdocCommandType.ARRAY_PUSH_LAST,
            path,
            ENCODER.encode(fragment).content(),
            createParent,
            xattr,
            expandMacro
    ));
    return this;
  }

  public <T> MutateInOps arrayPrepend(final boolean xattr, final String path, final T fragment) {
    return arrayPrepend(xattr, path, fragment, false, false);
  }

  public <T> MutateInOps arrayPrepend(final String path, final T fragment, final boolean createParent) {
    return arrayPrepend(false, path, fragment, createParent, false);
  }


  public <T> MutateInOps arrayPrepend(final String path, final T fragment) {
    return arrayPrepend(false, path, fragment, false, false);
  }

  public <T> MutateInOps arrayPrepend(final boolean xattr, final String path, final T fragment,
                                     final boolean createParent, final boolean expandMacro) {
    commands.add(new SubdocMutateRequest.Command(
            SubdocCommandType.ARRAY_PUSH_FIRST,
            path,
            ENCODER.encode(fragment).content(),
            createParent,
            xattr,
            expandMacro
    ));
    return this;
  }

  public <T> MutateInOps arrayInsert(final boolean xattr, final String path, final T fragment) {
    return arrayInsert(xattr, path, fragment, false, false);
  }

  public <T> MutateInOps arrayInsert(final String path, final T fragment, final boolean createParent) {
    return arrayInsert(false, path, fragment, createParent, false);
  }


  public <T> MutateInOps arrayInsert(final String path, final T fragment) {
    return arrayInsert(false, path, fragment, false, false);
  }

  public <T> MutateInOps arrayInsert(final boolean xattr, final String path, final T fragment,
                                      final boolean createParent, final boolean expandMacro) {
    commands.add(new SubdocMutateRequest.Command(
            SubdocCommandType.ARRAY_INSERT,
            path,
            ENCODER.encode(fragment).content(),
            createParent,
            xattr,
            expandMacro
    ));
    return this;
  }

  public <T> MutateInOps arrayAddUnique(final boolean xattr, final String path, final T fragment) {
    return arrayAddUnique(xattr, path, fragment, false, false);
  }

  public <T> MutateInOps arrayAddUnique(final String path, final T fragment, final boolean createParent) {
    return arrayAddUnique(false, path, fragment, createParent, false);
  }

  public <T> MutateInOps arrayAddUnique(final String path, final T fragment) {
    return arrayAddUnique(false, path, fragment, false, false);
  }

  public <T> MutateInOps arrayAddUnique(final boolean xattr, final String path, final T fragment,
                                     final boolean createParent, final boolean expandMacro) {
    commands.add(new SubdocMutateRequest.Command(
            SubdocCommandType.ARRAY_ADD_UNIQUE,
            path,
            ENCODER.encode(fragment).content(),
            createParent,
            xattr,
            expandMacro
    ));
    return this;
  }

  public <T> MutateInOps increment(final boolean xattr, final String path, final long delta) {
    return increment(xattr, path, delta, false);
  }

  public <T> MutateInOps increment(final String path, final long delta, final boolean createParent) {
    return increment(false, path, delta, createParent);
  }


  public <T> MutateInOps increment(final String path, final long delta) {
    return increment(false, path, delta, false);
  }

  public <T> MutateInOps increment(final boolean xattr, final String path, final long delta,
                                        final boolean createParent) {
    commands.add(new SubdocMutateRequest.Command(
            SubdocCommandType.COUNTER,
            path,
            ENCODER.encode(delta).content(),
            createParent,
            xattr,
            false
    ));
    return this;
  }

  public <T> MutateInOps decrement(final boolean xattr, final String path, final long delta) {
    return decrement(xattr, path, delta, false);
  }

  public <T> MutateInOps decrement(final String path, final long delta, final boolean createParent) {
    return decrement(false, path, delta, createParent);
  }


  public <T> MutateInOps decrement(final String path, final long delta) {
    return decrement(false, path, delta, false);
  }

  public <T> MutateInOps decrement(final boolean xattr, final String path, final long delta,
                                   final boolean createParent) {
    return increment(xattr, path, delta * -1, createParent);
  }


  @Stability.Internal
  public List<SubdocMutateRequest.Command> commands() {
    return commands;
  }

}

