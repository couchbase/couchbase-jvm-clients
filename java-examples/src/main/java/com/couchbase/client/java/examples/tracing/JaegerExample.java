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

package com.couchbase.client.java.examples.tracing;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.env.ClusterEnvironment;
import io.jaegertracing.Configuration;
import io.opentracing.Tracer;

public class JaegerExample {

  public static void main(String... args) throws Exception {
    Tracer tracer = Configuration
      .fromEnv("cb-ot-jaeger-example")
      .withSampler(Configuration.SamplerConfiguration.fromEnv().withType("const").withParam(1))
      .getTracer();

    Cluster cluster = Cluster.connect(ClusterEnvironment
      .builder("10.143.192.101", "Administrator", "password")
      .tracer(tracer)
      .build()
    );

    Bucket bucket = cluster.bucket("travel-sample");
    Collection collection = bucket.defaultCollection();
    System.err.println(collection.get("airline_10"));

    Thread.sleep(10000);
  }
}
