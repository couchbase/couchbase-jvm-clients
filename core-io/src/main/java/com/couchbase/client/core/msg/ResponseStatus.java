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

package com.couchbase.client.core.msg;

import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;

/**
 * The {@link ResponseStatus} describes what kind of response came back for a specific
 * request.
 *
 * <p>Note that this status is not tied to any protocol or service, rather there must
 * be a mapping performed from actual protocol-level response codes (be it http or
 * memcache protocol) to this abstract status. This allows to achieve a level of
 * consistency in status codes that is not tied to a particular protocol.</p>
 *
 * @since 1.0.0
 */
public enum ResponseStatus {
  /**
   * Indicates a successful response in general.
   */
  SUCCESS,
  /**
   * Indicates that the requested entity has not been found on the server.
   */
  NOT_FOUND,
  /**
   * Indicates an unknown status returned from the server, please check the
   * events/logs for further information.
   */
  UNKNOWN,
  /**
   * The server indicated that the given message failed because of a permission
   * violation.
   */
  NO_ACCESS;

  public boolean success() {
    return this == ResponseStatus.SUCCESS;
  }

}
