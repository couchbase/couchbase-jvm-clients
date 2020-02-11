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
import com.couchbase.client.core.error.ViewNotFoundException
import com.couchbase.client.scala.codec.JsonDeserializer.JsonObjectConvert
import com.couchbase.client.scala.json.JsonObject
import com.couchbase.client.scala.manager.view.{DesignDocument, View}
import com.couchbase.client.scala.transformers.JacksonTransformers
import com.couchbase.client.scala.{Bucket, Cluster, Collection, TestUtils}
import com.couchbase.client.scala.util.ScalaIntegrationTest
import com.couchbase.client.test.{ClusterType, IgnoreWhen, Util}
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test, TestInstance}
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
  }

  @AfterAll
  def afterAll(): Unit = {
    cluster.disconnect()
  }

  @Test
  def succeedsWithNoRowsReturned(): Unit = {
    val viewResult = bucket.viewQuery(DesignDocName, ViewName, ViewOptions().limit(0)).get
    assert(viewResult.rows.isEmpty)
    assert(viewResult.metaData.debugAs[JsonObject].isEmpty)
  }

  @Test
  def withDebug(): Unit = {
    val viewResult =
      bucket.viewQuery(DesignDocName, ViewName, ViewOptions().limit(0).debug(true)).get
    assert(viewResult.rows.isEmpty)
    assert(viewResult.metaData.debugAs[JsonObject].isDefined)
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

    println(viewResult)

    val count = viewResult.rows.count(_.id.get.startsWith("viewdoc-"))
    assert(count == docsToWrite)
    viewResult.rows.foreach(row => {
      assert(row.valueAs[JsonObject].get == JsonObject.create)
    })
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
