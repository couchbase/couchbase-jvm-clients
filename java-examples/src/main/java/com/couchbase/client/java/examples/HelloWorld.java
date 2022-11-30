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

/**
 * This example connects to a bucket, opens a collection, and performs a simple operation.
 * <p>
 * If this program does not run properly, make sure the "travel-sample" bucket is
 * present in your cluster. Also check the address in the connection string,
 * and the username and password.
 */
public class HelloWorld {

  public static void main(String... args) {

    // Connect to the cluster with a connection string and credentials.
    Cluster cluster = Cluster.connect("couchbase://127.0.0.1", "Administrator", "password");

    // Open an existing bucket.
    Bucket bucket = cluster.bucket("travel-sample");

    // Open a collection. Here it's the default collection, which also works with
    // old versions of Couchbase Server that do not support user-defined collections.
    Collection collection = bucket.defaultCollection();

    // Fetch a document from the travel-sample bucket.
    GetResult airport = collection.get("airport_1291");

    // Print the fetched document.
    System.err.println("*** Got document: " + airport);

    // Close network connections and release resources.
    cluster.disconnect();
  }
}
