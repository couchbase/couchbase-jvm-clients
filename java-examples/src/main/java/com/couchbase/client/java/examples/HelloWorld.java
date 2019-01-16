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

public class HelloWorld {

  public static void main(String... args) {
    Cluster cluster = Cluster.connect("127.0.0.1", "Administrator", "password");

    Bucket bucket = cluster.bucket("travel-sample");

    Collection collection = bucket.defaultCollection();

    Optional<GetResult> airport_10 = collection.get("airport_1291");

    System.err.println(airport_10);
  }
}
