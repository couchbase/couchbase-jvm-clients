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
package com.couchbase.client.scala.manager

import java.nio.charset.StandardCharsets
import java.util.UUID

import com.couchbase.client.core.error.IndexNotFoundException
import com.couchbase.client.scala.json.JsonObject
import com.couchbase.client.scala.manager.search._
import com.couchbase.client.scala.util.{CouchbasePickler, ScalaIntegrationTest}
import com.couchbase.client.scala.Cluster
import com.couchbase.client.test._
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api._

import scala.util.Failure

@TestInstance(Lifecycle.PER_CLASS)
@IgnoreWhen(missesCapabilities = Array(Capabilities.SEARCH))
class SearchIndexManagerSpec extends ScalaIntegrationTest {
  private var cluster: Cluster            = _
  private var bucketName: String          = _
  private var indexes: SearchIndexManager = _

  @BeforeAll
  def setup(): Unit = {
    cluster = connectToCluster()
    // Need to open a bucket until we have GCCCP support
    bucketName = ClusterAwareIntegrationTest.config().bucketname()
    val bucket = cluster.bucket(bucketName)
    indexes = cluster.searchIndexes
    indexes
      .getAllIndexes()
      .get
      .foreach(index => indexes.dropIndex(index.name))
  }

  @AfterAll
  def tearDown(): Unit = {
    indexes
      .getAllIndexes()
      .get
      .foreach(index => indexes.dropIndex(index.name))
    cluster.disconnect()
  }

  @Test
  def getIndexThatDoesNotExist(): Unit = {
    val result = indexes.getIndex("not_exist")

    result match {
      case Failure(err: IndexNotFoundException) =>
      case _                                    => assert(false)
    }
  }

  @Test
  def upsertIndex(): Unit = {
    val name  = "idx-" + UUID.randomUUID.toString.substring(0, 8)
    val index = SearchIndex.create(name, config.bucketname)
    indexes.upsertIndex(index).get

    val foundIndex = indexes.getIndex(name).get
    assert(name == foundIndex.name)
    assert(config.bucketname == foundIndex.sourceName)
  }

  @Test
  def upsertIndexTwice(): Unit = {
    val name  = "idx-" + UUID.randomUUID.toString.substring(0, 8)
    val index = SearchIndex.create(name, config.bucketname)
    indexes.upsertIndex(index).get

    // Need to get the UUID
    val foundIndex = indexes.getIndex(name).get

    assert(foundIndex.uuid.isDefined)
    indexes.upsertIndex(foundIndex).get
  }

  @Test
  def upsertIndexIsUnchanged(): Unit = {
    val name  = "idx-" + UUID.randomUUID.toString.substring(0, 8)
    val index = SearchIndex.create(name, config.bucketname)
    indexes.upsertIndex(index).get

    val foundIndex = indexes.getIndex(name).get

    indexes.upsertIndex(foundIndex).get

    val foundIndexAgain = indexes.getIndex(name).get

    // The UUID changes each time
    val fixIndex = foundIndex.copy(
      uuid = foundIndexAgain.uuid,
      numPlanPIndexes = foundIndexAgain.numPlanPIndexes
    )
    println(fixIndex)
    println(foundIndex)
    assert(fixIndex == foundIndexAgain)
  }

  @Test
  def parseIndex(): Unit = {
    val raw =
      """{"status":"ok","indexDef":{"type":"fulltext-index","name":"idx-18e0bf85","uuid":"6390517f1fa06371",
             "sourceType":"couchbase","sourceName":"1ccd5c20-73fd-44f1-8111-46bcac9203a2",
             "planParams":{"maxPartitionsPerPIndex":171},"params":{"doc_config":{"docid_prefix_delim":"",
             "docid_regexp":"","mode":"type_field","type_field":"type"},"mapping":{"analysis":{},
             "default_analyzer":"standard","default_datetime_parser":"dateTimeOptional","default_field":"_all",
             "default_mapping":{"dynamic":true,"enabled":true},"default_type":"_default","docvalues_dynamic":true,
             "index_dynamic":true,"store_dynamic":false,"type_field":"_type"},"store":{"indexType":"scorch",
             "kvStoreName":""}},"sourceParams":null},"planPIndexes":null,"warnings":null}"""

    val index = CouchbasePickler.read[SearchIndexWrapper](raw).indexDef
    assert(index.typ.contains("fulltext-index"))
    assert(index.uuid.contains("6390517f1fa06371"))
    val pp = index.planParamsAs[JsonObject].get
    assert(pp.num("maxPartitionsPerPIndex") == 171)
    val p = index.paramsAs[JsonObject].get
    assert(p.obj("doc_config").str("docid_regexp") == "")
  }

  @Test
  def getAllIndexes: Unit = {
    val name  = "idx-" + UUID.randomUUID.toString.substring(0, 8)
    val index = SearchIndex.create(name, config.bucketname)
    indexes.upsertIndex(index).get

    val allIndexes = indexes.getAllIndexes().get
    assert(allIndexes.exists(_.name == name))
  }

  @Test
  def parseAllIndexes(): Unit = {
    val raw =
      """{"status":"ok","indexDefs":{"uuid":"1893929e4a47ae29",
             "indexDefs":{"idx-3b687670":{"type":"fulltext-index","name":"idx-3b687670","uuid":"1893929e4a47ae29",
             "sourceType":"couchbase","sourceName":"e12c3ee1-2fb7-41b6-825a-4397865548dc",
             "planParams":{"maxPartitionsPerPIndex":171},"params":{"doc_config":{"docid_prefix_delim":"",
             "docid_regexp":"","mode":"type_field","type_field":"type"},"mapping":{"analysis":{},
             "default_analyzer":"standard","default_datetime_parser":"dateTimeOptional","default_field":"_all",
             "default_mapping":{"dynamic":true,"enabled":true},"default_type":"_default","docvalues_dynamic":true,
             "index_dynamic":true,"store_dynamic":false,"type_field":"_type"},"store":{"indexType":"scorch",
             "kvStoreName":""}},"sourceParams":null}},"implVersion":"5.5.0"}}"""
    val indexes = AsyncSearchIndexManager.parseIndexes(raw.getBytes(StandardCharsets.UTF_8))
    assert(indexes.size == 1)
  }

  @Test
  def dropIndex(): Unit = {
    val name  = "idx-" + UUID.randomUUID.toString.substring(0, 8)
    val index = SearchIndex.create(name, config.bucketname)
    indexes.upsertIndex(index).get

    indexes.dropIndex(name).get

    indexes.getIndex(name) match {
      case Failure(err: IndexNotFoundException) =>
      case _                                    => assert(false)
    }
  }

}
