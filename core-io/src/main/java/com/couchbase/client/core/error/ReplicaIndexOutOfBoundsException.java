/*
 * Copyright (c) 2026 Couchbase, Inc.
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

/**
 * Indicates a replica was requested by an index that is higher than the number of replicas configured for the bucket.
 */
public class ReplicaIndexOutOfBoundsException extends InvalidArgumentException {

  public ReplicaIndexOutOfBoundsException(String message) {
    super(message, null, null);
  }

  public static ReplicaIndexOutOfBoundsException forIndex(int requestedReplica, int numReplicas) {
    return new ReplicaIndexOutOfBoundsException(
        "Requested replica index " + requestedReplica + " but the bucket only has " + numReplicas + " replica(s) configured."
    );
  }

}
