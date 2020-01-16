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
import com.couchbase.client.test.ClusterAwareIntegrationTest
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api._

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

@TestInstance(Lifecycle.PER_CLASS)
class CouchbaseQueueSpec extends ScalaIntegrationTest {

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

  private def makeCollection = coll.queue[Int](docId)

  @Test
  def desired(): Unit = {
    val l = collection.mutable.Queue.empty[Int]

    try {
      l(0)
      assert(false)
    } catch {
      case err: IndexOutOfBoundsException =>
      case NonFatal(err)                  => assert(false)
    }

    try {
      l.dequeue()
      assert(false)
    } catch {
      case err: NoSuchElementException =>
      case NonFatal(err) =>
        println(err)
        assert(false)
    }
  }

  @Test
  def prepend(): Unit = {
    val l = makeCollection

    l.prepend(5)
    l.prepend(6)

    assert(l.toSeq == Seq(6, 5))

  }

  @Test
  def append(): Unit = {
    val l = makeCollection

    l.append(5)
    l.append(6)
    l += 7

    assert(l.toSeq == Seq(5, 6, 7))
  }

  @Test
  def dequeue(): Unit = {
    val l = makeCollection

    l.append(5)
    l.append(6)
    l += 7

    assert(l.dequeue() == 7)
    assert(l.size == 2)
    assert(l.dequeue() == 6)
    assert(l.size == 1)
    assert(l.dequeue() == 5)
    assert(l.size == 0)
  }

  @Test
  def count(): Unit = {

    val l = makeCollection

    assert(l.size == 0)

    l.append(5)

    assert(l.size == 1)

    l.append(6)

    assert(l.size == 2)
  }

  @Test
  def foreach(): Unit = {
    val l      = makeCollection
    val values = ArrayBuffer.empty[Int]

    l.foreach(v => values += v)

    assert(values.size == 0)

    l.append(5)
    l.append(6)

    l.foreach(v => values += v)

    assert(values.size == 2)

  }

  @Test
  def iterator(): Unit = {
    val l = makeCollection

    val itBefore = l.iterator

    assert(!itBefore.hasNext)

    l.append(5)
    l.append(6)

    val it = l.iterator

    assert(it.hasNext)
    assert(it.next() == 5)
    assert(it.next() == 6)
    assert(!it.hasNext)
  }

  @Test
  def get(): Unit = {
    val l = makeCollection

    l.append(5)
    l.append(6)

    assert(l(0) == 5)
    assert(l.head == 5)
    assert(l(1) == 6)
  }

  @Test
  def indexOf(): Unit = {
    val l = makeCollection

    l.append(5)
    l.append(6)
    l.append(7)

    assert(l.indexOf(5) == 0)
    assert(l.indexOf(6) == 1)
    assert(l.indexOf(100) == -1)
  }

  @Test
  def fancyStuff(): Unit = {
    // Extending the standard Scala Collections traits gives a lot of interesting stuff for free

    val l = makeCollection

    l.append(1)
    l.append(2)
    l.append(3)
    l.append(4)
    l.append(5)

    assert(l.take(2) == Seq(1, 2))
    assert(l.drop(2) == Seq(3, 4, 5))
    assert(l.nonEmpty)
    assert(l.toSet == Set(5, 3, 2, 1, 4))
    assert(l.mkString("[", ",", "]") == "[1,2,3,4,5]")
  }

  @Test
  def uncreatedColl(): Unit = {
    val l = makeCollection

    assert(l.size == 0)

    try {
      l(0)
      assert(false)
    } catch {
      case err: IndexOutOfBoundsException =>
      case NonFatal(err)                  => assert(false)
    }

    try {
      l.dequeue()
      assert(false)
    } catch {
      case err: NoSuchElementException =>
      case NonFatal(err)               => assert(false)
    }
  }

  @Test
  def getMissingElement(): Unit = {
    val l = makeCollection

    l.append(5)
    l.dequeue()

    try {
      l.dequeue()
      assert(false)
    } catch {
      case err: NoSuchElementException =>
      case NonFatal(err) =>
        println(err)
        assert(false)
    }
  }

}
