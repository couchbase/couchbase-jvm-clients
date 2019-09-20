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

package com.couchbase.client.java.examples.kv;

import com.couchbase.client.core.error.KeyNotFoundException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.kv.GetResult;

import static com.couchbase.client.java.kv.GetOptions.getOptions;

/**
 * This example shows all the different ways to use the get command on a collection.
 */
public class Get {

  public static void main(String... args) {
    /*
     * Connect to the cluster and open a collection to work with.
     */
    Cluster cluster = Cluster.connect("127.0.0.1", "Administrator", "password");
    Bucket bucket = cluster.bucket("travel-sample");
    Collection collection = bucket.defaultCollection();

    // -------------------------------------------------------------------------------------

    try {
      System.out.println("Found Airport: " + collection.get("airport_1291"));
    } catch (KeyNotFoundException ex) {
      System.out.println("Airport not found!");
    }

    // -------------------------------------------------------------------------------------

    /*
     * If only a couple fields of a document are needed (a projection), then this can be provided
     * through the options.
     */
    GetResult airline = collection.get(
      "airline_10",
      getOptions().project("airportname", "country")
    );

    System.out.println("Found airline with fields: " + airline);

    // -------------------------------------------------------------------------------------

    /*
     * In the previous examples, the expiration field has always been empty on the GetResult.
     * If you need to fetch the expiration time of a document, you can also pass it via the
     * options.
     */

    GetResult airline2 = collection.get("airline_10", getOptions().withExpiry(true));
    System.out.println("Expiration is: " + airline2.expiry());

  }
}
