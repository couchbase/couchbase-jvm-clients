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

import com.couchbase.client.core.msg.BaseResponse;
import com.couchbase.client.core.msg.ResponseStatus;

/**
 * Represents the response of a {@link GetMetaRequest}.
 *
 * @since 2.0.0
 */
public class GetMetaResponse extends KeyValueBaseResponse {

  private final long cas;
  private final boolean deleted;

  GetMetaResponse(final ResponseStatus status, final long cas, final boolean deleted) {
    super(status);
    this.cas = cas;
    this.deleted = deleted;
  }

  /**
   * Returns the CAS value of the document at the time of the fetch.
   */
  public long cas() {
    return cas;
  }

  /**
   * Returns true if the deleted flag is set in the extras.
   */
  public boolean deleted() {
    return deleted;
  }

  @Override
  public String toString() {
    return "GetMetaResponse{" +
      "status=" + status() +
      ", cas=" + cas +
      ", deleted=" + deleted +
      '}';
  }
}
