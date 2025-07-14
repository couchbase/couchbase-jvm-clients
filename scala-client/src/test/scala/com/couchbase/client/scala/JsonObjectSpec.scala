package com.couchbase.client.scala

import com.couchbase.client.scala.codec.Conversions
import com.couchbase.client.scala.json.{JsonArraySafe, JsonObject, JsonObjectSafe}
import org.junit.jupiter.api.{Assertions, Test}

class JsonObjectSpec {

  val raw =
    """{"name":"John Smith",
      |"age":29,
      |"address":[{"address":"123 Fake Street","regional":{"county:":"essex"}}]}""".stripMargin
  val json = JsonObject.fromJson(raw)
  @Test
  def decode(): Unit = {
    val encoded = Conversions.encode(json).get
    val str     = new String(encoded)
    val decoded = Conversions.decode[JsonObject](encoded).get

    assert(decoded.str("name") == "John Smith")
  }

  @Test
  def int_to_everything(): Unit = {
    val r = """{"hello":29}"""
    val j = JsonObject.fromJson(r)
    assert(j.num("hello") == 29)
    assert(j.numLong("hello") == 29)
    assert(j.numDouble("hello") == 29)
    assert(j.numFloat("hello") == 29)
    assert(j.str("hello") == "29")
    Assertions.assertThrows(classOf[RuntimeException], () => (j.bool("hello")))
    Assertions.assertThrows(classOf[RuntimeException], () => (j.obj("hello")))
    Assertions.assertThrows(classOf[RuntimeException], () => (j.arr("hello")))
    assert(j.toString == r)
  }

  @Test
  def double_to_everything(): Unit = {
    val r = """{"hello":29.34}"""
    val j = JsonObject.fromJson(r)
    assert(j.num("hello") == 29)
    assert(j.numLong("hello") == 29)
    assert(j.numDouble("hello") == 29.34)
    assert(j.numFloat("hello") == 29.34f)
    assert(j.str("hello") == "29.34")
    Assertions.assertThrows(classOf[RuntimeException], () => (j.bool("hello")))
    Assertions.assertThrows(classOf[RuntimeException], () => (j.obj("hello")))
    Assertions.assertThrows(classOf[RuntimeException], () => (j.arr("hello")))
    assert(j.toString == r)
  }

  @Test
  def obj_to_everything(): Unit = {
    val r = """{"hello":{"foo":"bar"}}"""
    val j = JsonObject.fromJson(r)
    assert(j.obj("hello").nonEmpty)
    Assertions.assertThrows(classOf[RuntimeException], () => (j.num("hello")))
    Assertions.assertThrows(classOf[RuntimeException], () => (j.numLong("hello")))
    Assertions.assertThrows(classOf[RuntimeException], () => (j.numDouble("hello")))
    Assertions.assertThrows(classOf[RuntimeException], () => (j.numFloat("hello")))
    Assertions.assertThrows(classOf[RuntimeException], () => (j.bool("hello")))
    Assertions.assertThrows(classOf[RuntimeException], () => (j.arr("hello")))
    assert(j.toString == r)
  }

  @Test
  def arr_to_everything(): Unit = {
    val r = """{"hello":["foo","bar"]}"""
    val j = JsonObject.fromJson(r)
    assert(j.arr("hello").nonEmpty)
    Assertions.assertThrows(classOf[RuntimeException], () => (j.num("hello")))
    Assertions.assertThrows(classOf[RuntimeException], () => (j.numLong("hello")))
    Assertions.assertThrows(classOf[RuntimeException], () => (j.numDouble("hello")))
    Assertions.assertThrows(classOf[RuntimeException], () => (j.numFloat("hello")))
    Assertions.assertThrows(classOf[RuntimeException], () => (j.bool("hello")))
    Assertions.assertThrows(classOf[RuntimeException], () => (j.obj("hello")))
    assert(j.toString == r)
  }

  @Test
  def safe(): Unit = {
    val raw = """[
                | {
                |  "appcode": "function OnUpdate(doc, meta) {}",
                |  "depcfg": {
                |   "source_bucket": "48844de0-56dd-4617-8d41-fb3131af6f63",
                |   "source_scope": "eventing",
                |   "source_collection": "source",
                |   "metadata_bucket": "48844de0-56dd-4617-8d41-fb3131af6f63",
                |   "metadata_scope": "eventing",
                |   "metadata_collection": "meta"
                |  },
                |  "version": "evt-7.0.1-6102-ee",
                |  "enforce_schema": false,
                |  "handleruuid": 217056801,
                |  "function_instance_id": "Kfqan",
                |  "appname": "04f87425-6d4e-4ad5-a0cc-741d76e9d146",
                |  "settings": {
                |   "cpp_worker_thread_count": 5,
                |   "dcp_stream_boundary": "from_now",
                |   "deployment_status": false,
                |   "description": "desc",
                |   "execution_timeout": 5,
                |   "language_compatibility": "6.0.0",
                |   "lcb_inst_capacity": 6,
                |   "lcb_retry_count": 7,
                |   "log_level": "DEBUG",
                |   "n1ql_consistency": "none",
                |   "processing_status": false
                |  }
                | }
                |]
                |""".stripMargin
    val json    = JsonArraySafe.fromJsonSafe(raw).get
    val str     = json.toString
    val jsonNew = JsonArraySafe.fromJsonSafe(str).get
    assert(json == jsonNew)
  }
}
