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

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.kv.GetResult;

import java.util.Optional;

/**
 * This example connects to a bucket, opens a collection and performs a simple operation.
 *
 * <p>If this program fails executing properly, there is a good chance your cluster is not set up
 * yet with the travel-sample bucket or you are pointing the code at the wrong hostname (or you
 * use wrong credentials).</p>
 */
public class HelloWorld {

  public static void main(String... args) {

    /*
     * Connect to the cluster with a hostname and credentials.
     */
    Cluster cluster = Cluster.connect("127.0.0.1", "Administrator", "password");

    /*
     * Open a bucket with the bucket name.
     */
    Bucket bucket = cluster.bucket("travel-sample");

    /*
     * Open a collection - here the default collection which is also backwards compatible to
     * servers which do not support collections.
     */
    Collection collection = bucket.defaultCollection();

    /*
     * Fetch a document from the travel-sample bucket.
     */
    Optional<GetResult> airport_10 = collection.get("airport_1291");

    /*
     * Print the fetched document.
     */
    System.err.println(airport_10);

  }
}
