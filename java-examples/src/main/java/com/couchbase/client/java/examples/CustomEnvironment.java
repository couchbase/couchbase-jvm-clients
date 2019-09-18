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
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.env.ClusterEnvironment;

import java.time.Duration;

import static com.couchbase.client.java.ClusterOptions.clusterOptions;

/**
 * This example shows how to configure a custom environment for the cluster.
 *
 *  * <p>If this program fails executing properly, there is a good chance your cluster is not set up
 *  * yet with the travel-sample bucket or you are pointing the code at the wrong hostname (or you
 *  * use wrong credentials).</p>
 */
public class CustomEnvironment {

    public static void main(String... args) throws Exception {

        /**
         * Creates a new environment and passes it into the cluster.
         *
         * You can see how the builder can be used to make changes programmatically.
         *
         * Please make sure to shut the environment down after the cluster manually if you are passing in a custom
         * one! It will shut down fine but you risk prematurely terminating outstanding operations.
         */
        ClusterEnvironment environment = ClusterEnvironment
                .builder()
                .timeoutConfig(TimeoutConfig.kvTimeout(Duration.ofSeconds(2)))
                .build();
        Cluster cluster = Cluster.connect("127.0.0.1", clusterOptions("Administrator", "password").environment(environment));

        /**
         * From here on everything works the same.
         */
        Bucket bucket = cluster.bucket("travel-sample");
        Collection collection = bucket.defaultCollection();


        /**
         * Do not forget to first shutdown the cluster and then also the environment afterwards!
         */
        cluster.shutdown();
        environment.shutdown();
    }
}
