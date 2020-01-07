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

import com.couchbase.client.core.error.DocumentNotFoundException
import com.couchbase.client.scala.env.ClusterEnvironment
import com.couchbase.client.scala.util.ScalaIntegrationTest
import com.couchbase.client.scala.{Cluster, Collection}
import com.couchbase.client.test.{ClusterAwareIntegrationTest, ClusterType, IgnoreWhen}
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api._

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

@TestInstance(Lifecycle.PER_CLASS)
@IgnoreWhen(clusterTypes = Array(ClusterType.MOCKED))
class CouchbaseSetSpec extends ScalaIntegrationTest {

  private var cluster: Cluster = _
  private var coll: Collection = _
  private val docId            = "test"

  @BeforeAll
  def beforeAll(): Unit = {
    cluster = connectToCluster()
    val bucket = cluster.bucket(config.bucketname)
    coll = bucket.defaultCollection

  }

  @AfterAll
  def afterAll(): Unit = {
    cluster.disconnect()
  }

  @BeforeEach
  def beforeEach(): Unit = {
    coll.remove(docId)
  }

  private def makeCollection = coll.set[Int](docId)

  @Test
  def desired(): Unit = {
    val l = collection.mutable.Set.empty[Int]

    try {
      l.head
      assert(false)
    } catch {
      case err: NoSuchElementException =>
      case NonFatal(err)               => assert(false)
    }

    l.remove(5)
    l -= 5
  }

  @Test
  def append(): Unit = {
    val l = makeCollection

    l += 5
    l += 6

    assert(l.toSeq == Seq(5, 6))

    l += 5

    assert(l.toSeq == Seq(5, 6))
  }

  @Test
  def remove(): Unit = {
    val l = makeCollection

    l += 5
    l += 6

    assert(l.toSeq == Seq(5, 6))

    l -= 5

    assert(l.toSeq == Seq(6))

    l -= 6

    assert(l.toSeq == Seq())
  }
  @Test
  def count(): Unit = {

    val l = makeCollection

    assert(l.size == 0)

    l += 5

    assert(l.size == 1)

    l += 6

    assert(l.size == 2)
  }

  @Test
  def foreach(): Unit = {
    val l      = makeCollection
    val values = ArrayBuffer.empty[Int]

    l.foreach(v => values += v)

    assert(values.size == 0)

    l += 5
    l += 6

    l.foreach(v => values += v)

    assert(values.size == 2)

  }

  @Test
  def iterator(): Unit = {
    val l = makeCollection

    val itBefore = l.iterator

    assert(!itBefore.hasNext)

    l += 5
    l += 6

    val it = l.iterator

    assert(it.hasNext)
    assert(it.next() == 5)
    assert(it.next() == 6)
    assert(!it.hasNext)
  }

  @Test
  def values(): Unit = {
    val l = makeCollection

    l += 5
    l += 6
    l += 7

    assert(l.toSeq.size == 3)
  }

  @Test
  def get(): Unit = {
    val l = makeCollection

    l += 5
    l += 6

    assert(l(5))
    assert(l(6))
    assert(!l(7))
  }
  @Test
  def uncreatedColl(): Unit = {
    val l = makeCollection

    assert(l.size == 0)

    try {
      l.head
      assert(false)
    } catch {
      case err: NoSuchElementException =>
      case NonFatal(err)               => assert(false)
    }

    assert(!l.remove(5))
    l -= 5
  }

  @Test
  def getMissingElement(): Unit = {
    val l = makeCollection

    l.add(5)
    l.remove(5)

    try {
      l.head
      assert(false)
    } catch {
      case err: NoSuchElementException =>
      case NonFatal(err)               => assert(false)
    }

    assert(!l.remove(5))
    l -= 5
  }
}
