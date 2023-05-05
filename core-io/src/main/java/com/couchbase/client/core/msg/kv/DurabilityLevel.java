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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.error.InvalidArgumentException;

/**
 * Specifies enhanced durability options for the mutation.
 */
public enum DurabilityLevel {
  /**
   * No enhanced durability configured for the mutation.
   */
  NONE((byte) 0x00),
  /**
   * The mutation must be replicated to (that is, held in the memory allocated to the bucket on) a majority of
   * the Data Service nodes.
   */
  MAJORITY((byte) 0x01),
  /**
   * The mutation must be replicated to a majority of the Data Service nodes.
   * <p>
   * Additionally, it must be persisted (that is, written and synchronised to disk) on the
   * node hosting the active partition (vBucket) for the data.
   */
  MAJORITY_AND_PERSIST_TO_ACTIVE((byte) 0x02),
  /**
   * The mutation must be persisted to a majority of the Data Service nodes.
   * <p>
   * Accordingly, it will be written to disk on those nodes.
   */
  PERSIST_TO_MAJORITY((byte) 0x03);

  private final byte code;

  DurabilityLevel(byte code) {
    this.code = code;
  }

  @Stability.Internal
  public byte code() {
    return code;
  }

  /**
   * Decodes the string representation of the durability level from the management API into an enum.
   *
   * @param input the management API string.
   * @return the encoded durability enum. Note that it will be NONE if input is null or unknown.
   */
  @Stability.Internal
  public static DurabilityLevel decodeFromManagementApi(final String input) {
    if (input == null) {
      return DurabilityLevel.NONE;
    }

    switch (input) {
      case "majority":
        return DurabilityLevel.MAJORITY;
      case "majorityAndPersistActive":
        return DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE;
      case "persistToMajority":
        return DurabilityLevel.PERSIST_TO_MAJORITY;
      default:
        return DurabilityLevel.NONE;
    }
  }

  /**
   * Encodes the {@link DurabilityLevel} so that the management API understands it.
   *
   * @return the encoded durability level for the management API.
   */
  @Stability.Internal
  public String encodeForManagementApi() {
    switch (this) {
      case NONE:
        return "none";
      case MAJORITY:
        return "majority";
      case MAJORITY_AND_PERSIST_TO_ACTIVE:
        return "majorityAndPersistActive";
      case PERSIST_TO_MAJORITY:
        return "persistToMajority";
      default:
        throw InvalidArgumentException.fromMessage("The provided durability level is not supported: " + this);
    }
  }
}
