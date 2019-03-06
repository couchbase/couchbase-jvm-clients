//package com.couchbase.client.scala
//
//import com.couchbase.client.scala.json.JsonObject
//import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.ObjectMapper
//import com.couchbase.client.core.deps.com.fasterxml.jackson.module.scala.DefaultScalaModule
//import com.jsoniter.JsonIterator
//import com.jsoniter.output.JsonStream
//import org.scalatest.FunSuite
//
//class JsoniterSpec extends FunSuite {
//
//
//  test("deserialise") {
//    val str = "{\"hello\":\"world\",\"foo\":\"bar\",\"age\":22}"
//        val it = JsonIterator.parse(str)
//        val any = it.readAny()
//    val map = any.asMap().asInstanceOf[java.util.HashMap[String,Any]]
//    val js = new JsonObject(map)
//
//    assert(js.str("hello") == "world")
//    assert(js.str("foo") == "bar")
//    assert(js.int("age") == 22)
//  }
//
//  def createJsonObject(str: String) = {
//    val it = JsonIterator.parse(str)
//    val any = it.readAny()
//    val map = any.asMap().asInstanceOf[java.util.HashMap[String,Any]]
//    val js = new JsonObject(map)
//    js
//  }
//
//  test("serialise") {
//    val str = "{\"hello\":\"world\",\"foo\":\"bar\",\"age\":22}"
//    val js = createJsonObject(str)
//
//    val gen = JsonStream.serialize(js.content)
//    val regen = createJsonObject(str)
//
//    assert(regen.str("hello") == "world")
//    assert(regen.str("foo") == "bar")
//    assert(regen.int("age") == 22)
//  }
//
//  test("serialise with added field") {
//    val str = "{\"hello\":\"world\",\"foo\":\"bar\",\"age\":22}"
//    val js = createJsonObject(str)
//
//    js.put("cool", "spot")
//
//    assert(js.str("cool") == "spot")
//
//    val gen = JsonStream.serialize(js.content)
//    val regen = createJsonObject(gen)
//
//    assert(regen.str("hello") == "world")
//    assert(regen.str("foo") == "bar")
//    assert(regen.str("cool") == "spot")
//    assert(regen.int("age") == 22)
//  }
//
//  //  test("deserialise array") {
////    val str = "{\"hello\":[\"world\",\"foo\"]}"
////    val it = JsonIterator.parse(str)
////    val any = it.readAny()
////    val map = any.asMap().asInstanceOf[java.util.HashMap[String,Any]]
////    val js = new JsonObject(map)
////
////    val arr = js.arr("hello")
////    assert(arr.size == 2)
////    assert(arr.get(0) == "world")
////    assert(arr.get(1) == "foo")
////  }
//
//
////  test("deserialise 2") {
////    val str = "{\"hello\":\"world\",\"foo\":\"bar\",\"age\":22}"
////    val it = JsonIterator.parse(str)
////
////    it.whatIsNext()
////    it.readObject()
////    it.readObjectCB()
////
////    val any = it.readAny()
////    val map = any.asMap().asInstanceOf[java.util.HashMap[String,Any]]
////    val js = new JsonObject(map)
////
////    assert(js.str("hello") == "world")
////    assert(js.str("foo") == "bar")
////    assert(js.int("age") == 22)
////  }
//}
