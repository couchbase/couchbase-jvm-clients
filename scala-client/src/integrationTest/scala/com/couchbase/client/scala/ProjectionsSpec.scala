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

  private def wrap(project: Seq[String]): JsonObject = {
    coll.get(docId, project = project) match {
      case Success(result) =>
        result.contentAs[JsonObject] match {
          case Success(body) => body
          case Failure(err) =>
            assert(false, s"unexpected error $err")
            throw new IllegalStateException()
        }
      case Failure(err) =>
        assert(false, s"unexpected error $err")
        throw new IllegalStateException()
    }
  }

  test("name") {
    val json = wrap(Seq("name"))
    assert(json.str("name") == "Emmy-lou Dickerson")
  }

  test("age") {
    val json = wrap(Seq("age"))
    assert(json.int("age") == 26)
  }

  test("animals") {
    val json = wrap(Seq("animals"))
    val arr = json.arr("animals")
    assert(arr.size == 3)
    assert(arr.toSeq == Seq("cat", "dog", "parrot"))
  }

  test("animals[0]") {
    val json = wrap(Seq("animals[0]"))
    val arr = json.arr("animals")
    assert(arr.toSeq == Seq("cat"))
  }

  test("animals[2]") {
    val json = wrap(Seq("animals[2]"))
    val arr = json.arr("animals")
    assert(arr.toSeq == Seq("parrot"))
  }

  test("attributes") {
    val json = wrap(Seq("attributes"))
    val field = json.obj("attributes")
    assert(field.size == 3)
    assert(field.arr("hobbies").obj(1).str("type") == "summer sports")
    assert(field.dyn.hobbies(1).name.str.get == "water skiing")
  }

  test("attributes.hair") {
    val json = wrap(Seq("attributes.hair"))
    assert(json.obj("attributes").str("hair") == "brown")
  }

  test("attributes.dimensions") {
    val json = wrap(Seq("attributes.dimensions"))
    assert(json.obj("attributes").obj("dimensions").int("height") == 67)
    assert(json.obj("attributes").obj("dimensions").int("weight") == 175)
  }

  test("attributes.dimensions.height") {
    val json = wrap(Seq("attributes.dimensions"))
    assert(json.obj("attributes").obj("dimensions").int("height") == 67)
  }

  test("attributes.hobbies[1].type") {
    val json = wrap(Seq("attributes.hobbies[1].type"))
    assert(json.obj("attributes").arr("hobbies").obj(0).str("type") == "summer sports")
  }

}