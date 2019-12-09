/*
 * Copyright (c) 2019 Couchbase, Inc.
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
package com.couchbase.client.scala.examples
import com.couchbase.client.core.env.PasswordAuthenticator
import com.couchbase.client.scala.env.ClusterEnvironment
import com.couchbase.client.scala.json.JsonObject
import com.couchbase.client.scala.{Cluster, ClusterOptions}
import com.couchbase.client.tracing.opentelemetry.OpenTelemetryRequestTracer
import io.opentelemetry.exporters.inmemory.InMemorySpanExporter
import io.opentelemetry.sdk.trace.TracerSdkFactory
import io.opentelemetry.sdk.trace.export.SimpleSpansProcessor

import scala.util.{Failure, Success}

object OpenTelemetry {
  def main(args: Array[String]): Unit = {
    // Create an OpenTelemetry Tracer
    val exporter = InMemorySpanExporter.create()
    val tracer   = TracerSdkFactory.create
    tracer.addSpanProcessor(SimpleSpansProcessor.newBuilder(exporter).build)

    // Provide that tracer to ClusterEnvironment
    ClusterEnvironment.builder
      .requestTracer(OpenTelemetryRequestTracer.wrap(tracer.get("integrationTest")))
      .build match {
      case Success(env) =>
        // Connect to a cluster - **CHANGE* these settings to your clusters'
        Cluster.connect(
          "10.112.178.101",
          ClusterOptions(PasswordAuthenticator.create("username", "password"), Some(env), None)
        ) match {
          case Success(cluster) =>
            // Open the default collection on bucket "default" (**CHANGE** this to the name of a test bucket that exists
            // on your cluster)
            val collection = cluster.bucket("default").defaultCollection

            // Do a simple key-value operation, to get some trace
            collection.upsert("test", JsonObject.create).get

            // Block until we get the trace
            var done = false
            while (!done) {
              if (exporter.getFinishedSpanItems.size >= 1) {
                done = true
              }
              Thread.sleep(50)
            }

            // Shut everything down
            cluster.disconnect()
            env.shutdown()

          case Failure(err) =>
            println(s"Failed to connect cluster with error: ${err}")
        }

      case Failure(err) =>
        println(s"Failed to create ClusterEnvironment error: ${err}")
    }
  }
}
