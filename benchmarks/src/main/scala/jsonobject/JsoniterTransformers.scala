package jsonobject

import java.util

import com.couchbase.client.scala.json.JsonObject
import com.jsoniter.output.JsonStream
import io.netty.util.CharsetUtil
import com.jsoniter.{JsonIterator, ValueType}
import experiments.JsoniterObject
//
object JsoniterTransformers {
  def encode(in: JsoniterObject): Array[Byte] = {
    val gen = JsonStream.serialize(in.content)
    gen.getBytes(CharsetUtil.UTF_8)
  }

  def decode(in: Array[Byte]): JsoniterObject = {
    val str = new String(in, CharsetUtil.UTF_8)
    val it = JsonIterator.parse(str)
    val any = it.readAny()
    val map = any.asMap().asInstanceOf[java.util.HashMap[String,Any]]
    val js = new JsoniterObject(map)
    js
    //    val it = JsonIterator.parse(str)
//
//    val next = it.whatIsNext()
//    if(next == ValueType.OBJECT) {
//      val map = new util.HashMap[String,Any]()
//      val n = it.whatIsNext()
//      if (n == ValueType.STRING) {
//        // TODO could use slice here
//        val t = it.readStringAsSlice()
//      }
//    }
//
//    val any = it.readAny()
//    if (any.valueType() == ValueType.OBJECT) {
//      val out = new JsonObjectExperiment(any.asMap().asInstanceOf[java.util.HashMap[String,Any]])
//
//    }
  }
}
