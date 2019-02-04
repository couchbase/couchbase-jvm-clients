package benchmarks

//import com.couchbase.client.java.codec.DefaultEncoder
//import com.couchbase.client.java.json.JsonObject
//import com.couchbase.client.scala.document.Conversions
//import org.openjdk.jmh.annotations.{Benchmark, Scope, Setup, State}
//import org.openjdk.jmh.runner.Runner
//import org.openjdk.jmh.runner.RunnerException
//import org.openjdk.jmh.runner.options.Options
//import org.openjdk.jmh.runner.options.OptionsBuilder
//import io.circe._
//import io.circe.generic.auto._
//import io.circe.parser._
//import io.circe.syntax._

//object Encoding {
//  @State(Scope.Thread)
//  class ThreadState {
//    var upickle: ujson.Obj = ujson.Obj("hello" -> "world", "foo" -> "bar", "age" -> 22)
////    var circe: io.circe.Json = null
////    var jsonObject: JsonObject = null
//
//  }
//}
//
//class Encoding {
//
//
//
////  @Setup def prepare: Unit = {
////     upickle =
//////    circe = Map("hello" -> "world",
//////      "foo" -> "bar",
//////      "age" -> 22).asJson
////    jsonObject = JsonObject.create()
////      .put("hello", "world")
////      .put("foo", "bar")
////      .put("age", 22)
////  }
//
//  @Benchmark
//  def upickleScala(state: Encoding.ThreadState): Unit = {
//    val encoded: Array[Byte] = Conversions.encode(state.upickle).get._1
//  }
//
////  @Benchmark def circeScala(): Unit = {
////    val encoded: Array[Byte] = Conversions.encode(circe).get._1
////  }
//
////  @Benchmark def jsoniterScala(): Unit = {
////    import com.github.plokhotnyuk.jsoniter_scala.macros._
////    import com.github.plokhotnyuk.jsoniter_scala.core._
////
////    val json: Array[Byte] = writeToArray(Map("hello" -> "world",
////    "foo" -> "bar",
////    "age" -> 22))
////    val encoded: Array[Byte] = Conversions.encode(json).get._1
////  }
//
////  @Benchmark def JsonObjectJava(): Unit = {
////    val encoded: Array[Byte] = DefaultEncoder.INSTANCE.encode(jsonObject).content()
////  }
//}