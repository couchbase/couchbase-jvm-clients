package com.couchbase.client.core.error;

public class CommonExceptions {
    private CommonExceptions() {}

    public static RuntimeException getFromReplicaNotCouchbaseBucket() {
        return new UnsupportedOperationException("Only Couchbase buckets are supported "
                + "for replica get requests!");
    }

    public static RuntimeException getFromReplicaInvalidReplica(int replicaIndex, int configuredReplicas) {
        return new UnsupportedOperationException("Not enough configured replicas (" + configuredReplicas + ") to satisfy" +
                " request for replica " + replicaIndex);
    }
}