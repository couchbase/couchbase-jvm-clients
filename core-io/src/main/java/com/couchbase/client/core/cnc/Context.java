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

package com.couchbase.client.core.cnc;

/**
 * Context represents some state that is passed throughout the system.
 *
 * <p>There are various stages of context that can extend or embed each other. The
 * important part is that it can be inspected and exported into other formats.</p>
 */
public interface Context {

  /**
   * Export this context into the specified format.
   *
   * @param format the format to export into.
   * @return the exported format as a string representation.
   */
  String exportAsString(final ExportFormat format);

  /**
   * The format into which the context can be exported.
   */
  enum ExportFormat {
    /**
     * Compact JSON.
     */
    JSON,

    /**
     * Verbose, Pretty JSON.
     */
    JSON_PRETTY,
    /**
     * Java "toString" basically.
     */
    STRING
  }
}
