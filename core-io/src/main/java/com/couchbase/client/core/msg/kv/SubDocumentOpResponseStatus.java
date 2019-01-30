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

package com.couchbase.client.core.msg.kv;

/**
 * The {@link SubDocumentOpResponseStatus} describes what kind of response came back for a specific
 * subdoc operation request.
 *
 * @since 2.0.0
 */
public enum SubDocumentOpResponseStatus {
  /**
   * Indicates a successful response in general.
   */
  SUCCESS,
  /**
   * The subdoc operation completed successfully on the deleted document
   */
  SUCCESS_DELETED_DOCUMENT,
  /**
   * The provided path does not exist in the document
   */
  PATH_NOT_FOUND,
  /**
   * One of path components treats a non-dictionary as a dictionary, or a non-array as an array, or value the path points to is not a number
   */
  PATH_MISMATCH,
  /**
   * The path's syntax was incorrect
   */
  PATH_INVALID,
  /**
   * The path provided is too large: either the string is too long, or it contains too many components
   */
  PATH_TOO_BIG,
  /**
   * The document has too many levels to parse
   */
  DOC_TOO_DEEP,
  /**
   * The value provided will invalidate the JSON if inserted
   */
  VALUE_CANTINSERT,
  /**
   * The existing document is not valid JSON
   */
  DOC_NOT_JSON,
  /**
   * The existing number is out of the valid range for arithmetic operations
   */
  NUM_RANGE,
  /**
   * The operation would result in a number outside the valid range
   */
  DELTA_RANGE,
  /**
   * The requested operation requires the path to not already exist, but it exists
   */
  PATH_EXISTS,
  /**
   * Inserting the value would cause the document to be too deep
   */
  VALUE_TOO_DEEP,
  /**
   * An invalid combination of commands was specified
   */
  INVALID_COMBO,
  /**
   * Specified key was successfully found, but one or more path operations failed
   */
  MULTI_PATH_FAILURE,
  /**
   * An invalid combination of operations, using macros when not using extended attributes
   */
  XATTR_INVALID_FLAG_COMBO,
  /**
   * Only single xattr key may be accessed at the same time
   */
  XATTR_INVALID_KEY_COMBO,
  /**
   * The server has no knowledge of the requested macro
   */
  XATTR_UNKNOWN_MACRO,
  /**
   * Unknown error.
   */
  UNKNOWN;

  public boolean success() {
    return this == SubDocumentOpResponseStatus.SUCCESS || this == SUCCESS_DELETED_DOCUMENT;
  }

}
