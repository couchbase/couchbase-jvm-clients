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

import java.util.Optional;

public class ObserveViaSeqnoResponse extends BaseResponse {

  private final boolean active;
  private final short vbucketID;
  private final long vbucketUUID;
  private final long lastPersistedSeqNo;
  private final long currentSeqNo;

  private final Optional<Long> oldVbucketUUID;
  private final Optional<Long> lastSeqNoReceived;

  public ObserveViaSeqnoResponse(final ResponseStatus status, final boolean active, final short vbucketID,
                                 final long vbucketUUID, final long lastPersistedSeqNo, final long currentSeqNo,
                                 final Optional<Long> oldVbucketUUID, final Optional<Long> lastSeqNoReceived) {
    super(status);
    this.active = active;
    this.vbucketID = vbucketID;
    this.vbucketUUID = vbucketUUID;
    this.lastPersistedSeqNo = lastPersistedSeqNo;
    this.currentSeqNo = currentSeqNo;
    this.oldVbucketUUID = oldVbucketUUID;
    this.lastSeqNoReceived = lastSeqNoReceived;
  }

  public boolean failedOver() {
    return oldVbucketUUID.isPresent();
  }

  public boolean active() {
    return active;
  }

  public short vbucketID() {
    return vbucketID;
  }

  public long vbucketUUID() {
    return vbucketUUID;
  }

  public long lastPersistedSeqNo() {
    return lastPersistedSeqNo;
  }

  public long currentSeqNo() {
    return currentSeqNo;
  }

  public Optional<Long> oldVbucketUUID() {
    return oldVbucketUUID;
  }

  public Optional<Long> lastSeqNoReceived() {
    return lastSeqNoReceived;
  }

  @Override
  public String toString() {
    return "ObserveViaSeqnoResponse{" +
      "status=" + status() +
      ", active=" + active +
      ", vbucketID=" + vbucketID +
      ", vbucketUUID=" + vbucketUUID +
      ", lastPersistedSeqNo=" + lastPersistedSeqNo +
      ", currentSeqNo=" + currentSeqNo +
      ", oldVbucketUUID=" + oldVbucketUUID +
      ", lastSeqNoReceived=" + lastSeqNoReceived +
      '}';
  }
}
