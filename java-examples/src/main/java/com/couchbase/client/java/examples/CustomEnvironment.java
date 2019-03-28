package com.couchbase.client.java.examples;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.env.ClusterEnvironment;

public class CustomEnvironment {

    public static void main(String... args) throws Exception {

        ClusterEnvironment environment = ClusterEnvironment
                .builder("127.0.0.1", "Administrator", "password")
                .build();
        Cluster cluster = Cluster.connect(environment);

        Bucket bucket = cluster.bucket("travel-sample");
        Collection collection = bucket.defaultCollection();

        Thread.sleep(2000);


        cluster.shutdown();

        for (int i = 0; i < 1024; i++) {
            System.err.println(collection.get("airport_100"));
        }

        environment.shutdown();
        Thread.sleep(1000);
    }
}
