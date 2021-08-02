/*
 * Copyright (c) 2016 Couchbase, Inc.
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

import java.io.Serializable;
import java.util.Objects;

import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;

/**
 * Value object to contain partition details and sequence number.
 *
 * @since 1.2.0
 */
public class MutationToken implements Serializable {

  private static final long serialVersionUID = 1014945571442656640L;

  private final short partitionID;
  private final long partitionUUID;
  private final long sequenceNumber;
  private final String bucketName;

  public MutationToken(short partitionID, long partitionUUID, long sequenceNumber, String bucketName) {
    this.partitionID = partitionID;
    this.partitionUUID = partitionUUID;
    this.sequenceNumber = sequenceNumber;
    this.bucketName = bucketName;
  }

  public long partitionUUID() {
    return partitionUUID;
  }

  public long sequenceNumber() {
    return sequenceNumber;
  }

  public short partitionID() {
    return partitionID;
  }

  public String bucketName() {
    return bucketName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MutationToken that = (MutationToken) o;
    return partitionID == that.partitionID &&
        partitionUUID == that.partitionUUID &&
        sequenceNumber == that.sequenceNumber &&
        Objects.equals(bucketName, that.bucketName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(partitionID, partitionUUID, sequenceNumber, bucketName);
  }

  @Override
  public String toString() {
    return "mt{" +
        "vbID=" + partitionID +
        ", vbUUID=" + partitionUUID +
        ", seqno=" + sequenceNumber +
        ", bucket=" + redactMeta(bucketName) +
        '}';
  }
}
