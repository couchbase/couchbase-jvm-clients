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
import com.couchbase.client.core.api.kv.CoreSubdocGetCommand;
import com.couchbase.client.core.msg.kv.SubdocCommandType;

/**
 * Defines specs to lookup parts in a JSON document.
 * <p>
 * Operations allow specifying an empty path ("") which means that the root document level is used (so it
 * will be applied to the full document). By nature it makes sense to only use such a command in isolation, but
 * it can be combined with xattr (extended attributes, a document metadata section) operations as well.
 */
public abstract class LookupInSpec {

  /**
   * Internal operation called from the encoding side that encodes the spec into its internal representation.
   */
  @Stability.Internal
  public abstract CoreSubdocGetCommand toCore();

  /**
   * Fetches the content from a field (if present) at the given path.
   *
   * @param path the path identifying where to get the value.
   * @return the created {@link LookupInSpec}.
   */
  public static LookupInSpecStandard get(final String path) {
    SubdocCommandType command = path.equals("") ? SubdocCommandType.GET_DOC : SubdocCommandType.GET;
    return new LookupInSpecStandard(command, path);
  }

  /**
   * Checks if a value at the given path exists in the document.
   *
   * @param path the path to check if the field exists.
   * @return the created {@link LookupInSpec}.
   */
  public static LookupInSpecStandard exists(final String path) {
    return new LookupInSpecStandard(SubdocCommandType.EXISTS, path);
  }

  /**
   * Counts the number of values at a given path in the document.
   *
   * @param path the path identifying where to count the values.
   * @return the created {@link LookupInSpec}.
   */
  public static LookupInSpecStandard count(final String path) {
    return new LookupInSpecStandard(SubdocCommandType.COUNT, path);
  }

}

