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
package com.couchbase.client.scala.encodings

import com.couchbase.client.scala.codec._
import com.couchbase.client.scala.env.ClusterEnvironment
import com.couchbase.client.scala.kv.InsertOptions
import com.couchbase.client.scala.util.ScalaIntegrationTest
import com.couchbase.client.scala.{Cluster, Collection, TestUtils}
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test, TestInstance}

import scala.concurrent.duration._
import scala.reflect.runtime.universe._
import scala.util.{Failure, Success, Try}

object TranscoderSpec {
  val Transcoder = new TranscoderWithSerializer {
    def encode[A](value: A, serializer: JsonSerializer[A]): Try[EncodedValue] = {
      JsonTranscoder.Instance.encode(value, serializer)
    }

    def decode[A](value: Array[Byte], flags: Int, serializer: JsonDeserializer[A])(
        implicit tag: WeakTypeTag[A]
    ): Try[A] = JsonTranscoder.Instance.decode(value, flags, serializer)
  }
}

@TestInstance(Lifecycle.PER_CLASS)
class TranscoderSpec extends ScalaIntegrationTest {

  private var cluster: Cluster = _
  private var coll: Collection = _

  override protected def environment: ClusterEnvironment.Builder = {
    ClusterEnvironment.builder.transcoder(TranscoderSpec.Transcoder)
  }

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

  @Test
  def insert(): Unit = {
    val docId = TestUtils.docId()
    coll.remove(docId)
    val content = ujson.Obj("hello" -> "world")
    assert(coll.insert(docId, content).isSuccess)

    coll.get(docId) match {
      case Success(result) =>
        assert(result.transcoder == TranscoderSpec.Transcoder)
        result.contentAs[ujson.Obj] match {
          case Success(body) =>
            assert(body("hello").str == "world")
          case Failure(err) => assert(false, s"unexpected error $err")
        }
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  @Test
  def get_and_touch(): Unit = {
    val docId = TestUtils.docId()
    coll.remove(docId)
    val content      = ujson.Obj("hello" -> "world")
    val insertResult = coll.insert(docId, content, InsertOptions().expiry(10.seconds)).get

    assert(insertResult.cas != 0)

    coll.get(docId) match {
      case Success(result) =>
        assert(result.transcoder == TranscoderSpec.Transcoder)
        assert(result.contentAs[ujson.Obj].get == content)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

}
