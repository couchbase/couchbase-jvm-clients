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

import com.couchbase.client.core.msg.BaseResponse;
import com.couchbase.client.core.msg.ResponseStatus;

public class ObserveViaCasResponse extends BaseResponse {

  private final long cas;
  private final ObserveStatus observeStatus;
  private final boolean active;
  private final ResponseStatusDetails statusDetails;

  public ObserveViaCasResponse(ResponseStatus status, long cas, ObserveStatus observeStatus, final boolean active,
                               final ResponseStatusDetails statusDetails) {
    super(status);
    this.cas = cas;
    this.observeStatus = observeStatus;
    this.active = active;
    this.statusDetails = statusDetails;
  }

  public long cas() {
    return cas;
  }

  public ObserveStatus observeStatus() {
    return observeStatus;
  }

  public boolean active() {
    return active;
  }

  public ResponseStatusDetails statusDetails() {
    return statusDetails;
  }

  public enum ObserveStatus {
    /**
     * Observe status not known.
     */
    UNKNOWN((byte) 0xf0),
    /**
     * Response indicating the key was uninitialized.
     */
    UNINITIALIZED((byte) 0xff),
    /**
     * Response indicating the key was modified.
     */
    MODIFIED((byte) 0xfe),
    /**
     * Response indicating the key was persisted.
     */
    FOUND_PERSISTED((byte) 0x01),
    /**
     * Response indicating the key was found but not persisted.
     */
    FOUND_NOT_PERSISTED((byte) 0x00),
    /**
     * Response indicating the key was not found and persisted, as in
     * the case of deletes - a real delete.
     */
    NOT_FOUND_PERSISTED((byte) 0x80),
    /**
     * Response indicating the key was not found and not
     * persisted, as in the case of deletes - a logical delete.
     */
    NOT_FOUND_NOT_PERSISTED((byte) 0x81);

    private final byte value;

    ObserveStatus(byte b) {
      value = b;
    }

    public static ObserveStatus valueOf(byte b) {
      switch (b) {
        case (byte) 0x00:
          return ObserveStatus.FOUND_NOT_PERSISTED;
        case (byte) 0x01:
          return ObserveStatus.FOUND_PERSISTED;
        case (byte) 0x80:
          return ObserveStatus.NOT_FOUND_PERSISTED;
        case (byte) 0x81:
          return ObserveStatus.NOT_FOUND_NOT_PERSISTED;
        case (byte) 0xfe:
          return ObserveStatus.MODIFIED;
        case (byte) 0xf0:
          return ObserveStatus.UNKNOWN;
        default:
          return ObserveStatus.UNINITIALIZED;
      }
    }

    public byte value() {
      return value;
    }
  }

  @Override
  public String toString() {
    return "ObserveViaCasResponse{" +
      "cas=" + cas +
      ", observeStatus=" + observeStatus +
      ", active=" + active +
      ", statusDetails=" + statusDetails +
      '}';
  }
}
