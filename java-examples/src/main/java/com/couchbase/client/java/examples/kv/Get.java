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

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.kv.GetResult;

import java.util.Optional;

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

    /*
     * The get command returns an Optional to signal if the document is present.
     *
     * This might be a bit verbose to work with upfront, but it saves headaches in production which
     * would otherwise return null or throw an exception at runtime.
     */
    Optional<GetResult> airport = collection.get("airport_1291");
    if (airport.isPresent()) {
      System.out.println("Found Airport: " + airport.get());
    } else {
      System.out.println("Airport not found!");
    }

    // -------------------------------------------------------------------------------------

    /*
     * If only a couple fields of a document are needed (a projection), then this can be provided
     * through the options.
     */
    Optional<GetResult> airline = collection.get(
      "airline_10",
      getOptions().project("airportname", "country")
    );

    /*
     * You can utilize the Java 8+ lambda functions on the Optional for ease of use.
     */
    airline.ifPresent(result -> System.out.println("Found airline with fields: " + result));

    // -------------------------------------------------------------------------------------

    /*
     * In the previous examples, the expiration field has always been empty on the GetResult.
     * If you need to fetch the expiration time of a document, you can also pass it via the
     * options.
     */

    collection.get("airline_10", getOptions().withExpiration(true))
      .ifPresent(getResult -> System.out.println("Expiration is: " + getResult.expiration()));

  }
}
