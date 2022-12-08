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
package com.couchbase.client.scala.view
import java.util.concurrent.TimeUnit
import com.couchbase.client.core.error.ViewNotFoundException
import com.couchbase.client.core.service.ServiceType
import com.couchbase.client.scala.TestUtils.waitForService
import com.couchbase.client.scala.codec.JsonDeserializer.JsonObjectConvert
import com.couchbase.client.scala.json.{JsonArray, JsonObject}
import com.couchbase.client.scala.manager.view.{DesignDocument, View}
import com.couchbase.client.scala.transformers.JacksonTransformers
import com.couchbase.client.scala.{Bucket, Cluster, Collection, TestUtils}
import com.couchbase.client.scala.util.ScalaIntegrationTest
import com.couchbase.client.test.{ClusterType, Flaky, IgnoreWhen, Util}
import org.junit.jupiter.api.{AfterAll, BeforeAll, Disabled, Test, TestInstance}
import org.junit.jupiter.api.TestInstance.Lifecycle

import scala.concurrent.duration.Duration._
import scala.util.Failure
import scala.concurrent.duration._

@TestInstance(Lifecycle.PER_CLASS)
class ViewSpec extends ScalaIntegrationTest {

  private var cluster: Cluster = _
  private var coll: Collection = _
  private var bucket: Bucket   = _
  private val DesignDocName    = "everything"
  private val ViewName         = "all"

  @BeforeAll
  def beforeAll(): Unit = {
    cluster = connectToCluster()

    bucket = cluster.bucket(config.bucketname)
    coll = bucket.defaultCollection
    bucket.waitUntilReady(WaitUntilReadyDefault)

    val designDoc = DesignDocument(
      DesignDocName,
      Map(
        ViewName -> View("function(doc,meta) { emit(meta.id, doc) }")
      )
    )
    bucket.viewIndexes.upsertDesignDocument(designDoc, DesignDocumentNamespace.Production).get
    Util.waitUntilCondition(() => {
      bucket.viewIndexes
        .getDesignDocument(DesignDocName, DesignDocumentNamespace.Production)
        .isSuccess
    })
    // Extra wait for service for CI test stability
    waitForService(bucket, ServiceType.VIEWS)
  }

  @AfterAll
  def afterAll(): Unit = {
    cluster.disconnect()
  }

  @Test
  def succeedsWithNoRowsReturned(): Unit = {
    Util.waitUntilCondition(() => {
      try {
        val viewResult = bucket.viewQuery(DesignDocName, ViewName, ViewOptions().limit(0)).get
        assert(viewResult.rows.isEmpty)
        assert(viewResult.metaData.debugAs[JsonObject].isEmpty)
        true
      } catch {
        case _: ViewNotFoundException =>
          // Intermittently on CI see this error despite previous queries on the same view succeeding.
          // Presumably hitting different nodes.
          println("Looping on ViewNotFoundException")
          false
      }
    })
  }

  @Test
  def withDebug(): Unit = {
    Util.waitUntilCondition(() => {
      try {
        val viewResult =
          bucket.viewQuery(DesignDocName, ViewName, ViewOptions().limit(0).debug(true)).get
        assert(viewResult.rows.isEmpty)
        assert(viewResult.metaData.debugAs[JsonObject].isDefined)
        true
      } catch {
        case _: ViewNotFoundException =>
          // Intermittently on CI see this error despite previous queries on the same view succeeding.
          // Presumably hitting different nodes.
          println("Looping on ViewNotFoundException")
          false
      }
    })
  }

  @Test
  def parse() = {
    val raw        = """{"id":"viewdoc-0","key":"viewdoc-0","value":{}}"""
    val json       = JacksonTransformers.MAPPER.readTree(raw)
    val value      = json.get("value")
    val writer     = JacksonTransformers.MAPPER.writer
    val bytes      = writer.writeValueAsBytes(value)
    val jsonObject = JsonObjectConvert.deserialize(bytes).get
  }

  @Test
  def badParam(): Unit = {
    val viewResult =
      bucket.viewQuery("not-exist", ViewName)

    viewResult match {
      case Failure(err: ViewNotFoundException) =>
      case _                                   =>
    }
  }

  @Test
  def badParamReactive(): Unit = {
    val viewResult = bucket.reactive.viewQuery("not-exist", ViewName)

    try {
      val vr1 = viewResult.block()
      assert(false)
    } catch {
      case err: ViewNotFoundException =>
    }
  }

  // See this fail intermittently on CI as the returned docs don't equal what was just written - which should not be
  // possible with RequestPlus.
  @Disabled @Flaky
  @Test
  @IgnoreWhen(clusterTypes = Array(ClusterType.MOCKED))
  def returnsDataJustWritten(): Unit = {
    val docsToWrite = 10
    for (x <- Range(0, docsToWrite)) {
      coll.upsert("viewdoc-" + x, JsonObject.create).get
    }

    val viewResult = bucket
      .viewQuery(
        DesignDocName,
        ViewName,
        ViewOptions().scanConsistency(ViewScanConsistency.RequestPlus)
      )
      .get

    val useful = viewResult.rows.filter(_.id.get.startsWith("viewdoc-"))
    assert(useful.size == docsToWrite)
    useful.foreach(row => {
      assert(row.valueAs[JsonObject].get == JsonObject.create)
    })
  }

  @Disabled @Flaky // See the final result contain no rows intermittently - though this should be impossible
  @Test
  @IgnoreWhen(clusterTypes = Array(ClusterType.MOCKED))
  def canQueryWithKeysPresent(): Unit = {
    val docsToWrite = 2
    for (x <- Range(0, docsToWrite)) {
      coll.upsert("keydoc-" + x, JsonObject.create).get
    }

    val viewResult = bucket
      .viewQuery(
        DesignDocName,
        ViewName,
        ViewOptions()
          .scanConsistency(ViewScanConsistency.RequestPlus)
          .keys(Seq("keydoc-0", "keydoc-1"))
      )
      .get

    println(viewResult)
    assert(viewResult.rows.size == 2)
  }

  @Test
  @IgnoreWhen(clusterTypes = Array(ClusterType.MOCKED))
  def returnsDataJustWrittenReactive(): Unit = {
    val docsToWrite = 10
    for (x <- Range(0, docsToWrite)) {
      coll.upsert("viewdoc-" + x, JsonObject.create).get
    }

    val viewResult = bucket.reactive
      .viewQuery(
        DesignDocName,
        ViewName,
        ViewOptions().scanConsistency(ViewScanConsistency.RequestPlus)
      )
      .block()

    val rows = viewResult.rows.collectSeq.block()
    assert(rows.count(_.id.get.startsWith("viewdoc-")) == 10)
  }
}
