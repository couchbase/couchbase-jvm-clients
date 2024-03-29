/*
 * Copyright (c) 2022 Couchbase, Inc.
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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.msg.ResponseStatus;

/**
 * This is an internal exception used to signal exceptional states in the range scan orchestrator.
 */
@Stability.Internal
public class RangeScanPartitionFailedException extends CouchbaseException {

  private final ResponseStatus status;

  public RangeScanPartitionFailedException(String message, ResponseStatus status) {
    super(message);
    this.status = status;
  }

  public ResponseStatus status() {
    return status;
  }

}
