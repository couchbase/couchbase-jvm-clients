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

package com.couchbase.client.core.error;

import com.couchbase.client.core.cnc.Context;

/**
 * The parent class for all exceptions created by this SDK (or clients using it).
 *
 * @since 2.0.0
 */
public class CouchbaseException extends RuntimeException {

  private final ErrorContext ctx;

  /**
   * Keeping it in there to not break left and right, but must be removed eventually to force good errors.
   */
  @Deprecated
  public CouchbaseException() {
    this.ctx = null;
  }

  /**
   * Keeping it in there to not break left and right, but must be removed eventually to force good errors.
   */
  @Deprecated
  public CouchbaseException(Throwable cause) {
    super(cause);
    this.ctx = null;
  }

  public CouchbaseException(final String message) {
    this(message, (ErrorContext) null);
  }

  public CouchbaseException(final String message, final ErrorContext ctx) {
    super(message);
    this.ctx = ctx;
  }

  public CouchbaseException(final String message, final Throwable cause) {
    this(message, cause, null);
  }

  public CouchbaseException(final String message, final Throwable cause, final ErrorContext ctx) {
    super(message, cause);
    this.ctx = ctx;
  }

  @Override
  public String toString() {
    final String output = super.toString();
    return ctx != null ? output + " " + ctx.exportAsString(Context.ExportFormat.JSON) : output;
  }

  /**
   * Returns the error context, if present.
   */
  public ErrorContext context() {
    return ctx;
  }

}
