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
package com.couchbase.client.scala.datastructures

import java.util.concurrent.TimeUnit
import com.couchbase.client.core.error.DocumentNotFoundException
import com.couchbase.client.scala.env.ClusterEnvironment
import com.couchbase.client.scala.util.ScalaIntegrationTest
import com.couchbase.client.scala.{Cluster, Collection}
import com.couchbase.client.test.{ClusterAwareIntegrationTest, IgnoreWhen}
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api._

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

@IgnoreWhen(isProtostellarWillWorkLater = true) // Needs ING-372
@TestInstance(Lifecycle.PER_CLASS)
class CouchbaseMapSpec extends ScalaIntegrationTest {

  private var cluster: Cluster = _
  private var coll: Collection = _
  private val docId            = "test"

  @BeforeAll
  def beforeAll(): Unit = {
    cluster = connectToCluster()
    val bucket = cluster.bucket(config.bucketname)
    coll = bucket.defaultCollection
    bucket.waitUntilReady(WaitUntilReadyDefault)
  }

  @AfterAll
  def afterAll(): Unit = {
    cluster.disconnect()
  }

  @BeforeEach
  def beforeEach(): Unit = {
    coll.remove(docId)
  }

  private def makeCollection = coll.map[Int](docId)

  @Test
  def desired(): Unit = {
    val l = collection.mutable.Map.empty[String, Int]

    try {
      l("key1")
      assert(false)
    } catch {
      case err: NoSuchElementException =>
      case NonFatal(err)               => assert(false)
    }

    l -= "key1"
    assert(l.remove("key1").isEmpty)
  }

  @Test
  def exists(): Unit = {
    val l = makeCollection

    l += "key1" -> 5
    l += "key2" -> 6

    assert(l.contains("key1"))
    assert(l.contains("key2"))
    assert(!l.contains("key3"))
  }

  @Test
  def append(): Unit = {
    val l = makeCollection

    l += "key1" -> 5
    l += "key2" -> 6

    assert(l("key1") == 5)
    assert(l("key2") == 6)
  }

  @Test
  def appendDouble(): Unit = {
    val l = makeCollection

    l += "key1" -> 5
    l += "key1" -> 6

    assert(l("key1") == 6)
    assert(l.size == 1)
  }
  @Test
  def remove(): Unit = {
    val l = makeCollection

    l += "key1" -> 5
    l += "key2" -> 6

    l -= "key1"

    assert(!l.contains("key1"))
    assert(l.contains("key2"))

    l -= "key2"

    assert(!l.contains("key2"))

    l -= "key3"
  }

  @Test
  def count(): Unit = {

    val l = makeCollection

    assert(l.size == 0)

    l += "key1" -> 5

    assert(l.size == 1)

    l += "key2" -> 6

    assert(l.size == 2)
  }

  @Test
  def foreach(): Unit = {
    val l      = makeCollection
    val values = ArrayBuffer.empty[Int]

    l.foreach(v => values += v._2)

    assert(values.size == 0)

    l += "key1" -> 5
    l += "key2" -> 6

    l.foreach(v => values += v._2)

    assert(values.size == 2)

  }

  @Test
  def iterator(): Unit = {
    val l = makeCollection

    val itBefore = l.iterator

    assert(!itBefore.hasNext)

    l += "key1" -> 5
    l += "key2" -> 6

    val it = l.iterator

    assert(it.hasNext)
    var next = it.next()
    assert(next._1 == "key1")
    assert(next._2 == 5)
    next = it.next()
    assert(next._1 == "key2")
    assert(next._2 == 6)
    assert(!it.hasNext)
  }

  @Test
  def uncreatedColl(): Unit = {
    val l = makeCollection

    assert(l.size == 0)
    assert(l.get("no_exist").isEmpty)
    assert(l.remove("no_exist").isEmpty)
    l.removeAt("no_exist")
    l -= "no_exist"
  }
  @Test
  def getMissingElement(): Unit = {
    val l = makeCollection

    l += "key1" -> 5

    assert(l.get("no_exist").isEmpty)

    try {
      l("no_exist")
      assert(false)
    } catch {
      case err: NoSuchElementException =>
    }

    assert(l.remove("no_exist").isEmpty)
    l.removeAt("no_exist")
    l -= "no_exist"
  }

  @Test
  def get(): Unit = {
    val l = makeCollection

    l += "key1" -> 5
    l += "key2" -> 6

    assert(l("key1") == 5)
    assert(l.head._2 == 5)
    assert(l("key2") == 6)
  }
}
