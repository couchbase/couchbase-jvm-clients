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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.msg.ResponseStatus;

/**
 * Represents the response of a {@link GetRequest}.
 *
 * @since 2.0.0
 */
@Stability.Internal
public final class GetResponse extends KeyValueBaseResponse {

  private final byte[] content;
  private final long cas;
  private final int flags;

  public GetResponse(final ResponseStatus status, final byte[] content, final long cas, final int flags) {
    super(status);
    this.content = content;
    this.cas = cas;
    this.flags = flags;
  }

  /**
   * Returns the content, but might be empty or null.
   */
  public byte[] content() {
    return content;
  }

  /**
   * Returns the CAS value of the document at the time of the fetch.
   */
  public long cas() {
    return cas;
  }

  /**
   * Returns the flags of this document, if found.
   */
  public int flags() {
    return flags;
  }

}
