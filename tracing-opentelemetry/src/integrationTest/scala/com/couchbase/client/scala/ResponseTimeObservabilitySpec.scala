/*
 * Copyright (c) 2020 Couchbase, Inc.
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
package com.couchbase.client.scala

import java.util.UUID

import com.couchbase.client.scala.env.ClusterEnvironment
import com.couchbase.client.test._
import com.couchbase.client.tracing.opentelemetry.OpenTelemetryRequestTracer
import io.opentelemetry.exporters.inmemory.InMemorySpanExporter
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.OpenTelemetry
import io.opentelemetry.sdk.trace.TracerSdkManagement
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api.{io, _}
import _root_.io.opentelemetry.sdk.trace.`export`.SimpleSpanProcessor

@TestInstance(Lifecycle.PER_CLASS)
class ResponseTimeObservabilitySpec extends ClusterAwareIntegrationTest {

  private val exporter = InMemorySpanExporter.create()

  private var cluster: Cluster = _
  private var coll: Collection = _
  private var env: ClusterEnvironment  = _
  private var tracer: TracerSdkManagement = _

  @BeforeAll
  def beforeAll(): Unit = {
    val config = ClusterAwareIntegrationTest.config()
    val nodeConfig = config.firstNodeWith(Services.KV).get

    env = {
      tracer = OpenTelemetrySdk.getTracerManagement
      tracer.addSpanProcessor(SimpleSpanProcessor.newBuilder(exporter).build())

      ClusterEnvironment.builder
        .requestTracer(OpenTelemetryRequestTracer.wrap(OpenTelemetry.getTracer("integrationTest"))).build.get
    }

    cluster = Cluster.connect(
      "couchbase://" + nodeConfig.hostname() + ":" + nodeConfig.ports().get(Services.KV),
      ClusterOptions.create(config.adminUsername(), config.adminPassword()).environment(env)
    ).get

    val bucket = cluster.bucket(config.bucketname)
    coll = bucket.defaultCollection
  }

  @AfterAll
  def afterAll(): Unit = {
    cluster.disconnect()
    env.shutdown()
    tracer.shutdown()
  }

  @BeforeEach
  def beforeEach() = {
    exporter.reset()
  }

  private def waitForEvents(numEvents: Int): Unit = {
    Util.waitUntilCondition(() => {
      val size = exporter.getFinishedSpanItems().size()
      size == numEvents
    })
    exporter.reset()
  }

  @Test
  def basicKV() {
    val docId = UUID.randomUUID().toString

    val content = ujson.Obj("hello" -> "world")
    coll.insert(docId, content).get
    waitForEvents(3)

    coll.get(docId).get
    waitForEvents(2)

    coll.remove(docId)
    waitForEvents(2)
  }

  @Test
  def reactiveKV() {
    val docId = UUID.randomUUID().toString

    val content = ujson.Obj("hello" -> "world")
    coll.reactive.insert(docId, content).block()
    waitForEvents(3)

    coll.reactive.get(docId).block()
    waitForEvents(2)

    coll.reactive.remove(docId).block()
    waitForEvents(2)
  }
  @Test
  @IgnoreWhen(missesCapabilities = Array(Capabilities.QUERY))
  def query() {
    cluster.query("select 'hello' as greeting").get
    waitForEvents(1)

    cluster.reactive.query("select 'hello' as greeting").block()
    waitForEvents(1)
  }

  @Test
  @IgnoreWhen(missesCapabilities = Array(Capabilities.ANALYTICS))
  def analytics() {
    cluster.analyticsQuery("select 'hello' as greeting").get
    waitForEvents(1)

    cluster.reactive.analyticsQuery("select 'hello' as greeting").block()
    waitForEvents(1)
  }
}
