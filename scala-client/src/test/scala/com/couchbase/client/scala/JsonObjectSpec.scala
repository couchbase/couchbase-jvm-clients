package com.couchbase.client.scala

import com.couchbase.client.scala.document.{JsonArray, JsonObject}
import org.scalatest.{FlatSpec, FunSuite}

class JsonObjectSpec extends FunSuite {

  test("field = 'value'") {
    val obj = JsonObject.create
    val out = obj.put("field", "value")
    assert(out.field.exists)
    assert(out.field.path.toString == "field")
    assert(out.field.getString == "value")
  }


  // TODO MVP
//  test("field1.field2 = 'value'") {
//    val obj = JsonObject.create
//    val out = obj.put("field1", JsonObject.create.put("field2", "value"))
//    assert(out.field1.exists)
//    assert(out.field1.field2.exists)
//    assert(out.field1.field2.path.toString == "field1.field2")
//    assert(out.field1.field2.getString == "value")
//  }
//
//  test("field1.field2 = 42") {
//    val obj = JsonObject.create
//    val out = obj.put("field1", JsonObject.create.put("field2", 42))
//    assert(out.field1.exists)
//    assert(out.field1.field2.exists)
//    assert(out.field1.field2.getInt == 42)
//    intercept[ClassCastException] {
//      assert(out.field1.field2.getString == 42)
//    }
//  }
//
//  test("field1.field2.field3 = 'value'") {
//    val obj = JsonObject.create
//    val out = obj.put("field1", JsonObject.create.put("field2", JsonObject.create.put("field3", "value")))
//    assert(out.field1.exists)
//    assert(out.field1.field2.exists)
//    assert(out.field1.field2.field3.exists)
//    assert(out.field1.field2.field3.getString == "value")
//  }
//
//  test("field1(0).field2 = 'value'") {
//    val obj = JsonObject.create
//    val out = obj.put("field1", JsonArray.from(JsonObject.create.put("field2", "value")))
//    assert(out.field1.exists)
//    assert(out.field1(0).exists)
//    assert(out.field1(0).path.toString == "field1[0]")
//    assert(out.field1(0).field2.exists)
//    assert(out.field1(0).field2.path.toString == "field1[0].field2")
//    assert(out.field1(0).field2.getString == "value")
//  }
//
//  test("field1(0).field2(0) = 'value'") {
//    val obj = JsonObject.create
//    val out = obj.put("field1", JsonArray.from(JsonArray.from(JsonObject.create.put("field2", "value"))))
//    assert(out.field1.exists)
//    assert(out.field1(0).field2(0).exists)
//    assert(out.field1(0).field2(0).path.toString == "field1[0].field2[0]")
//    assert(out.field1(0).field2(0).field2.getString == "value")
//  }

}
