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

import com.couchbase.client.core.msg.kv.DurabilityLevel;

/**
 * This exception is raised when a durability level has been requested that is not available on the server.
 *
 * @since 2.0.0
 */
public class DurabilityLevelNotAvailableException extends CouchbaseException {

  public DurabilityLevelNotAvailableException() {
  }

  public DurabilityLevelNotAvailableException(String message) {
    super(message);
  }

  public DurabilityLevelNotAvailableException(String message, Throwable cause) {
    super(message, cause);
  }

  public DurabilityLevelNotAvailableException(Throwable cause) {
    super(cause);
  }

  public DurabilityLevelNotAvailableException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }

  public DurabilityLevelNotAvailableException(DurabilityLevel level) {
    super("Durability level " + level + " is not supported by this version of the server");
  }
}
