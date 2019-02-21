package com.couchbase.client.scala

import com.couchbase.client.scala.json.JsonObject
import org.scalatest.FunSuite

import scala.util.{Failure, Success}

class ProjectionsSpec extends FunSuite {
  val (cluster, bucket, coll) = (for {
    cluster <- Cluster.connect("localhost", "Administrator", "password")
    bucket <- cluster.bucket("default")
    coll <- bucket.defaultCollection()
  } yield (cluster, bucket, coll)) match {
    case Success(result) => result
    case Failure(err) => throw err
  }

  val raw = """{
              |    "name": "Emmy-lou Dickerson",
              |    "age": 26,
              |    "animals": ["cat", "dog", "parrot"],
              |    "attributes": {
              |        "hair": "brown",
              |        "dimensions": {
              |            "height": 67,
              |            "weight": 175
              |        },
              |        "hobbies": [{
              |                "type": "winter sports",
              |                "name": "curling"
              |            },
              |            {
              |                "type": "summer sports",
              |                "name": "water skiing",
              |                "details": {
              |                    "location": {
              |                        "lat": 49.282730,
              |                        "long": -123.120735
              |                    }
              |                }
              |            }
              |        ]
              |    }
              |}
              |""".stripMargin
  val docId = "projection-test"
  coll.upsert(docId, raw)

  test("name") {
    coll.get(docId) match {
      case Success(result) =>
        result.contentAs[ujson.Obj] match {
          case Success(body) =>
            assert(body("hello").str == "world")
          case Failure(err) => assert(false, s"unexpected error $err")
        }
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }
}