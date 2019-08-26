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

package com.couchbase.client.java.examples.view;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.view.ViewResult;

import static com.couchbase.client.java.view.ViewOptions.viewOptions;

/**
 * This example shows how to query a view with custom options in a blocking fashion and to consume the rows
 * and the associated metadata.
 */
public class ViewExample {

    public static void main(String... args) {
        /*
         * Connect to the cluster with a hostname and credentials.
         */
        Cluster cluster = Cluster.connect("127.0.0.1", "Administrator", "password");

        /*
         * Open a bucket with the bucket name.
         */
        Bucket bucket = cluster.bucket("beer-sample");

        /*
         * Performs the view query
         */
        ViewResult result = bucket.viewQuery("beer", "brewery_beers", viewOptions().limit(2));

        /*
         * Prints metadata like the total number of rows.
         */
        System.err.println(result.meta());

        /*
         * Iterates the rows and prints the information from each row
         */
        result.rows().forEach(r -> {
            System.out.println("-- Row --");
            System.out.println("ToString(): " + r);
            System.out.println("ID: " + r.id());
            System.out.println("Key: " + r.keyAs(JsonArray.class));
            System.out.println("Value: " + r.valueAs(JsonObject.class));
        });
    }
}
