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
import com.couchbase.client.core.msg.kv.SubdocMutateRequest;
import com.couchbase.client.java.codec.DefaultEncoder;
import com.couchbase.client.java.codec.Encoder;

import java.util.ArrayList;
import java.util.List;

public class MutateInSpec {
  private final SubdocMutateRequest.Command command;
    
  private MutateInSpec(SubdocMutateRequest.Command command) {
    this.command = command;
  }
    
  private static final Encoder ENCODER = new DefaultEncoder();

  @Stability.Internal
  public SubdocMutateRequest.Command command() {
    return command;
  }

  public static <T> MutateInSpec replace(final boolean xattr, final String path, final T fragment) {
    return replace(xattr, path, fragment, false, false);
  }

  public static <T> MutateInSpec replace(final String path, final T fragment, final boolean createParent) {
    return replace(false, path, fragment, createParent, false);
  }

  public static <T> MutateInSpec replace(final String path, final T fragment) {
    return replace(false, path, fragment, false, false);
  }

  public static <T> MutateInSpec replace(final boolean xattr, final String path, final T fragment,
                                  final boolean createParent, final boolean expandMacro) {
    return new MutateInSpec(new SubdocMutateRequest.Command(
      SubdocCommandType.REPLACE,
      path,
      ENCODER.encode(fragment).content(),
      createParent,
      xattr,
      expandMacro
    ));
  }

  public static <T> MutateInSpec insert(final boolean xattr, final String path, final T fragment) {
    return insert(xattr, path, fragment, false, false);
  }

  public static <T> MutateInSpec insert(final String path, final T fragment, final boolean createParent) {
    return insert(false, path, fragment, createParent, false);
  }


  public static <T> MutateInSpec insert(final String path, final T fragment) {
    return insert(false, path, fragment, false, false);
  }

  public static <T> MutateInSpec insert(final boolean xattr, final String path, final T fragment,
                                 final boolean createParent, final boolean expandMacro) {
    return new MutateInSpec(new SubdocMutateRequest.Command(
      SubdocCommandType.DICT_ADD,
      path,
      ENCODER.encode(fragment).content(),
      createParent,
      xattr,
      expandMacro
    ));
  }

  public static <T> MutateInSpec remove(final String path) {
    return remove(false, path);
  }

  public static <T> MutateInSpec remove(final boolean xattr, final String path) {
    return new MutateInSpec(new SubdocMutateRequest.Command(
            SubdocCommandType.DELETE,
            path,
            new byte[] {},
            false,
            xattr,
            false
    ));
  }


  public static <T> MutateInSpec upsert(final boolean xattr, final String path, final T fragment) {
    return upsert(xattr, path, fragment, false, false);
  }

  public static <T> MutateInSpec upsert(final String path, final T fragment, final boolean createParent) {
    return upsert(false, path, fragment, createParent, false);
  }


  public static <T> MutateInSpec upsert(final String path, final T fragment) {
    return upsert(false, path, fragment, false, false);
  }

  public static <T> MutateInSpec upsert(final boolean xattr, final String path, final T fragment,
                                 final boolean createParent, final boolean expandMacro) {
    return new MutateInSpec(new SubdocMutateRequest.Command(
            SubdocCommandType.DICT_UPSERT,
            path,
            ENCODER.encode(fragment).content(),
            createParent,
            xattr,
            expandMacro
    ));
  }


    public static <T> MutateInSpec upsertFullDocument(final T fragment) {
        return new MutateInSpec(new SubdocMutateRequest.Command(
                SubdocCommandType.UPSERTDOC,
                "",
                ENCODER.encode(fragment).content(),
                false,
                false,
                false
        ));
    }


    public static <T> MutateInSpec arrayAppend(final boolean xattr, final String path, final T fragment) {
    return arrayAppend(xattr, path, fragment, false, false);
  }

  public static <T> MutateInSpec arrayAppend(final String path, final T fragment, final boolean createParent) {
    return arrayAppend(false, path, fragment, createParent, false);
  }


  public static <T> MutateInSpec arrayAppend(final String path, final T fragment) {
    return arrayAppend(false, path, fragment, false, false);
  }

  public static <T> MutateInSpec arrayAppend(final boolean xattr, final String path, final T fragment,
                                      final boolean createParent, final boolean expandMacro) {
    return new MutateInSpec(new SubdocMutateRequest.Command(
            SubdocCommandType.ARRAY_PUSH_LAST,
            path,
            ENCODER.encode(fragment).content(),
            createParent,
            xattr,
            expandMacro
    ));
  }

  public static <T> MutateInSpec arrayPrepend(final boolean xattr, final String path, final T fragment) {
    return arrayPrepend(xattr, path, fragment, false, false);
  }

  public static <T> MutateInSpec arrayPrepend(final String path, final T fragment, final boolean createParent) {
    return arrayPrepend(false, path, fragment, createParent, false);
  }


  public static <T> MutateInSpec arrayPrepend(final String path, final T fragment) {
    return arrayPrepend(false, path, fragment, false, false);
  }

  public static <T> MutateInSpec arrayPrepend(final boolean xattr, final String path, final T fragment,
                                       final boolean createParent, final boolean expandMacro) {
    return new MutateInSpec(new SubdocMutateRequest.Command(
            SubdocCommandType.ARRAY_PUSH_FIRST,
            path,
            ENCODER.encode(fragment).content(),
            createParent,
            xattr,
            expandMacro
    ));
  }

  public static <T> MutateInSpec arrayInsert(final boolean xattr, final String path, final T fragment) {
    return arrayInsert(xattr, path, fragment, false, false);
  }

  public static <T> MutateInSpec arrayInsert(final String path, final T fragment, final boolean createParent) {
    return arrayInsert(false, path, fragment, createParent, false);
  }


  public static <T> MutateInSpec arrayInsert(final String path, final T fragment) {
    return arrayInsert(false, path, fragment, false, false);
  }

  public static <T> MutateInSpec arrayInsert(final boolean xattr, final String path, final T fragment,
                                      final boolean createParent, final boolean expandMacro) {
    return new MutateInSpec(new SubdocMutateRequest.Command(
            SubdocCommandType.ARRAY_INSERT,
            path,
            ENCODER.encode(fragment).content(),
            createParent,
            xattr,
            expandMacro
    ));
  }

  public static <T> MutateInSpec arrayAddUnique(final boolean xattr, final String path, final T fragment) {
    return arrayAddUnique(xattr, path, fragment, false, false);
  }

  public static <T> MutateInSpec arrayAddUnique(final String path, final T fragment, final boolean createParent) {
    return arrayAddUnique(false, path, fragment, createParent, false);
  }

  public static <T> MutateInSpec arrayAddUnique(final String path, final T fragment) {
    return arrayAddUnique(false, path, fragment, false, false);
  }

  public static <T> MutateInSpec arrayAddUnique(final boolean xattr, final String path, final T fragment,
                                         final boolean createParent, final boolean expandMacro) {
    return new MutateInSpec(new SubdocMutateRequest.Command(
            SubdocCommandType.ARRAY_ADD_UNIQUE,
            path,
            ENCODER.encode(fragment).content(),
            createParent,
            xattr,
            expandMacro
    ));
  }

  public static <T> MutateInSpec increment(final boolean xattr, final String path, final long delta) {
    return increment(xattr, path, delta, false);
  }

  public static <T> MutateInSpec increment(final String path, final long delta, final boolean createParent) {
    return increment(false, path, delta, createParent);
  }


  public static <T> MutateInSpec increment(final String path, final long delta) {
    return increment(false, path, delta, false);
  }

  public static <T> MutateInSpec increment(final boolean xattr, final String path, final long delta,
                                    final boolean createParent) {
    return new MutateInSpec(new SubdocMutateRequest.Command(
            SubdocCommandType.COUNTER,
            path,
            ENCODER.encode(delta).content(),
            createParent,
            xattr,
            false
    ));
  }

  public static <T> MutateInSpec decrement(final boolean xattr, final String path, final long delta) {
    return decrement(xattr, path, delta, false);
  }

  public static <T> MutateInSpec decrement(final String path, final long delta, final boolean createParent) {
    return decrement(false, path, delta, createParent);
  }

  public static <T> MutateInSpec decrement(final String path, final long delta) {
    return decrement(false, path, delta, false);
  }

  public static <T> MutateInSpec decrement(final boolean xattr, final String path, final long delta,
                                    final boolean createParent) {
    return increment(xattr, path, delta * -1, createParent);
  }
}

