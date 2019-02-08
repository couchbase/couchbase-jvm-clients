package jsonobject

import java.util

import jawn.SimpleFacade

import collection.JavaConverters._

// Doesn't work due to jfalse
//class JawnToJsonObject extends SimpleFacade[jsonobject.JsonObject] {
//  def jnull() = null
//  def jfalse() = false
//  def jtrue() = true
//  def jnum(s: String) = s.toDouble
//  def jint(s: String) = s.toInt
//  def jstring(s: String) = s
//  def jarray(vs: List[Any]) = JsonArray(vs.asJava)
//  def jobject(vs: Map[String, Any]) = {
//    val map = new util.HashMap[String, Any](vs.size)
//    vs.foreach(v => map.put(v._1, v._2))
//    JsonObject(map)
//  }
//}