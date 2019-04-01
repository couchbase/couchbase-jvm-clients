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

package com.couchbase.client.java.examples.query;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryResult;

import static com.couchbase.client.java.query.QueryOptions.queryOptions;

public class ParameterizedQueryExample {

  public static void main(String... args) {
    Cluster cluster = Cluster.connect("127.0.0.1", "Administrator", "password");
    Bucket bucket = cluster.bucket("travel-sample");

    QueryResult result = cluster.query(
      "select * from `travel-sample` where type = ? limit 5",
      queryOptions().withParameters(JsonArray.from("airport"))
    );
    System.out.println(result.rowsAsObject());

    result = cluster.query(
      "select * from `travel-sample` where type = $type limit 5",
      queryOptions().withParameters(JsonObject.empty().put("type", "airport"))
    );
    System.out.println(result.rowsAsObject());

  }

}
