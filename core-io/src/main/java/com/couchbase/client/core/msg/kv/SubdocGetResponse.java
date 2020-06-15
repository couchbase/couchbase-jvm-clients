/*
 * Copyright (c) 2019 Couchbase, Inc.
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

import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.msg.BaseResponse;
import com.couchbase.client.core.msg.ResponseStatus;

import java.util.Arrays;
import java.util.Optional;

public class SubdocGetResponse extends BaseResponse {

  private final SubDocumentField[] values;
  private final long cas;
  private final Optional<CouchbaseException> error;
  private final boolean isDeleted;

  public SubdocGetResponse(ResponseStatus status,
                           Optional<CouchbaseException> error,
                           SubDocumentField[] values,
                           long cas,
                           final boolean isDeleted) {
    super(status);
    this.error = error;
    this.values = values;
    this.cas = cas;
    this.isDeleted = isDeleted;
  }

  public SubDocumentField[] values() {
    return values;
  }

  public long cas() {
    return cas;
  }

  /**
   * Error will be set, and should be checked and handled, when status==SUBDOC_FAILURE
   */
  public Optional<CouchbaseException> error() { return error; }

  public boolean isDeleted() {
    return isDeleted;
  }

  @Override
  public String toString() {
    return "SubdocGetResponse{" +
      "error=" + error +
      "values=" + Arrays.asList(values) +
      ", cas=" + cas +
      ", isDeleted=" + isDeleted +
      '}';
  }
}
