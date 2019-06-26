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

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;

import static com.couchbase.client.java.kv.GetOptions.getOptions;
import static com.couchbase.client.java.kv.UpsertOptions.upsertOptions;

public class EntityUpsertAndGet {

  public static void main(String... args) {
    /*
     * Connect to the cluster and open a collection to work with.
     */
    Cluster cluster = Cluster.connect("127.0.0.1", "Administrator", "password");
    Bucket bucket = cluster.bucket("travel-sample");
    Collection collection = bucket.defaultCollection();

    final String id = "p01";
    Person person = new Person(
      "Joan Deere",
      28,
      Arrays.asList("kitty", "puppy"),
      new Person.Attributes(
        "brown",
        new Person.Dimensions(65, 120),
        Collections.singletonList(new Person.Hobby(
          "winter",
          "Curling",
          new Person.Details(new Person.Location(37.338207, -121.886330))
        ))
      )
    );

    collection.upsert(
      id,
      person,
      upsertOptions().expiry(Duration.ofDays(1))
    );

    GetResult getResult = collection.get(id, getOptions().timeout(Duration.ofSeconds(10)));
    Person read = getResult.contentAs(Person.class);
    System.out.println("Person read: " + read);

  }

}
