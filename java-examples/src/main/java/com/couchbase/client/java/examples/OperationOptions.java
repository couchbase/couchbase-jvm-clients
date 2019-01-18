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

import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.CommonOptions;
import io.opentracing.Span;

import java.time.Duration;

import static com.couchbase.client.java.kv.GetOptions.getOptions;

/**
 * This example shows the common options available for all (most) data operations.
 *
 * <p>Every data operation inherits from {@link CommonOptions} which provides a consistent way
 * to specify certain options like timeouts and retry strategy.</p>
 */
public class OperationOptions {

  public static void main(String... arg) {
    /*
     * Connect to the cluster and open a collection to work with.
     */
    Cluster cluster = Cluster.connect("127.0.0.1", "Administrator", "password");
    Bucket bucket = cluster.bucket("travel-sample");
    Collection collection = bucket.defaultCollection();

    /*
     * You can configure a custom timeout for each operation which will override the default.
     */
    collection.get(
      "airline_10",
      getOptions().timeout(Duration.ofSeconds(1))
    );

    /*
     * It is also possible to configure a custom retry strategy on a per-operation basis.
     */
    collection.get(
      "airline_10",
      getOptions().retryStrategy(BestEffortRetryStrategy.INSTANCE)
    );

    /*
     * Finally, you can set an opentracing span as a parent for the operation.
     */
    Span parent = null; // this would be your parent span
    collection.get(
      "airline_10",
      getOptions().parentSpan(parent)
    );
  }
}
