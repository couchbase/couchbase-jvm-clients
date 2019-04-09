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

package com.couchbase.client.java.examples.manage;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.manager.SearchIndexManager;
import com.couchbase.client.java.manager.SearchIndex;

/**
 * This example shows how to load, create, update and remove search indexes.
 *
 *
 */
public class SearchIndexManagement {

  public static void main(String... args) throws Exception {
    Cluster cluster = Cluster.connect("127.0.0.1", "Administrator", "password");
    Bucket bucket = cluster.bucket("travel-sample");

    /*
     * Provides access to the index manager.
     */
    SearchIndexManager manager = cluster.searchIndexes();

    /*
     * Loads an index from cluster if it exists (will fail otherwise).
     */
    SearchIndex index = manager.get("ts");

    /*
     * Replaces the loaded index on the server.
     */
    manager.replace(index);

    /*
     * Creates a new index with a new name and inserts it on the server.
     */
    manager.insert(SearchIndex.from("ts2", index));

    /*
     * Creates an index and removes it from the server.
     */
    manager.insert(SearchIndex.from("ts3", index));
    manager.remove("ts3");
  }

}
