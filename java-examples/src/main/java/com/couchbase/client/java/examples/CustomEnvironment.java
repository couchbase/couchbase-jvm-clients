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

package com.couchbase.client.java.examples;

import com.couchbase.client.core.env.TimeoutConfig;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.env.ClusterEnvironment;

import java.time.Duration;

import static com.couchbase.client.java.ClusterOptions.clusterOptions;

/**
 * This example shows how to configure a custom environment for the cluster.
 * <p>
 * If this program fails, check that:
 * <ul>
 *   <li>Your cluster has the "travel-sample" sample bucket installed.
 *   <li>The call to Cluster.connect() uses the correct hostname and credentials.
 * </ul>
 */
public class CustomEnvironment {

    public static void main(String... args) throws Exception {
        // Alternate ways to configure a cluster environment.
        simpleEnvironmentConfiguration();
        shareEnvironmentBetweenClusters();
    }

    /**
     * If you only need one Cluster, or if you need multiple Clusters with
     * different configurations, customize the environment when creating
     * the Cluster(s) like in this example.
     */
    public static void simpleEnvironmentConfiguration() {
        Cluster cluster = Cluster.connect("127.0.0.1", clusterOptions("Administrator", "password")
            // This version of the "environment" method takes a callback
            // that configures the ClusterEnvironment.Builder for the environment
            // owned by this cluster.
            .environment(env -> env
                .timeoutConfig(it -> it
                  .kvTimeout(Duration.ofSeconds(2))
                )
            ));

        Bucket bucket = cluster.bucket("travel-sample");
        Collection collection = bucket.defaultCollection();

        // [amazing application code here]

        // When you're done with the cluster, disconnect it to release resources.
        // Because we didn't configure the cluster with a pre-built ClusterEnvironment,
        // the cluster owns its environment and will automatically shut it down.
        cluster.disconnect();
    }

    /**
     * In this example we're going to build a ClusterEnvironment ourselves
     * and share it between multiple clusters.
     * <p>
     * Because we are creating the shared environment, we are responsible for
     * shutting it down after all the clusters that use it have disconnected.
     * <p>
     * <b>CAUTION:</b> Failing to shut down the shared environment results
     * in a resource leak.
     */
    public static void shareEnvironmentBetweenClusters() {

        ClusterEnvironment sharedEnvironment = ClusterEnvironment.builder()
            .timeoutConfig(TimeoutConfig.kvTimeout(Duration.ofSeconds(2)))
            .build(); // We built the environment, so we are responsible for shutting it down!

        Cluster cluster1 = Cluster.connect("127.0.0.1", clusterOptions("Administrator", "password")
            .environment(sharedEnvironment));

        Cluster cluster2 = Cluster.connect("127.0.0.1", clusterOptions("Administrator", "password")
            .environment(sharedEnvironment));

        // From here on everything works the same.
        Bucket bucket1 = cluster1.bucket("travel-sample");
        Bucket bucket2 = cluster2.bucket("travel-sample");
        Collection collection1 = bucket1.defaultCollection();
        Collection collection2 = bucket2.defaultCollection();

        // [amazing application code here]

        /*
         * Do not forget to first shut down the clusters and then also the environment afterwards!
         */
        cluster1.disconnect();
        cluster2.disconnect();
        sharedEnvironment.shutdown();
    }
}
