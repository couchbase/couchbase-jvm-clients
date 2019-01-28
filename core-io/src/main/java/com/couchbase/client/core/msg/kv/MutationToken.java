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

/**
 * Value object to contain vbucket UUID and sequence number.
 *
 * @author Michael Nitschinger
 * @since 1.2.0
 */
public class MutationToken {

    private final short vbucketID;
    private final long vbucketUUID;
    private final long sequenceNumber;
    private final String bucket;

    public MutationToken(short vbucketID, long vbucketUUID, long sequenceNumber, String bucket) {
        this.vbucketID = vbucketID;
        this.vbucketUUID = vbucketUUID;
        this.sequenceNumber = sequenceNumber;
        this.bucket = bucket;
    }

    public long vbucketUUID() {
        return vbucketUUID;
    }

    public long sequenceNumber() {
        return sequenceNumber;
    }

    public short vbucketID() {
        return vbucketID;
    }

    public String bucket() {
        return bucket;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MutationToken that = (MutationToken) o;

        if (vbucketID != that.vbucketID) return false;
        if (vbucketUUID != that.vbucketUUID) return false;
        if (sequenceNumber != that.sequenceNumber) return false;
        return bucket != null ? bucket.equals(that.bucket) : that.bucket == null;

    }

    @Override
    public int hashCode() {
        int result = (int) (vbucketID ^ (vbucketID >>> 32));
        result = 31 * result + (int) (vbucketUUID ^ (vbucketUUID >>> 32));
        result = 31 * result + (int) (sequenceNumber ^ (sequenceNumber >>> 32));
        result = 31 * result + (bucket != null ? bucket.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("mt{");
        sb.append("vbID=").append(vbucketID);
        sb.append(", vbUUID=").append(vbucketUUID);
        sb.append(", seqno=").append(sequenceNumber);
        sb.append(", bucket=").append(bucket);
        sb.append('}');
        return sb.toString();
    }
}
