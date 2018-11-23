package com.couchbase.client.scala

import org.scalatest.{FlatSpec, FunSuite}

import scala.reflect.ClassTag

case class User(name: String, age: Int)

//class GetExperimentSpec extends FunSuite {
//
//
////  test("target") {
////    def get[T](id: String,
////               options: GetOptions = GetOptions(),
////               target: Class[T] = classOf[JsonObject]): Document[T] = null
////
////    val result = get("hello")            // Document[JsonObject]
////
////    val result2 = get[User]("hello")     // Document[User]
////  }
//
//  test("ClassTag") {
//    def get[T : ClassTag](id: String,
//    options: GetOptions = GetOptions())
//                         (implicit tag: ClassTag[T] = Class[JsonObject]): Document[T] = null
//
//    val result = get("id")
//
//
//    val result2 = get[User]("id")
//
//
//  }
//
//  test("ClassTag2") {
//    // https://webcache.googleusercontent.com/search?q=cache:TB6NqiDqBFcJ:https://www.cakesolutions.net/teamblogs/default-type-parameters-with-implicits-in-scala%3FhsFormKey%3D1551af45ea16a306e5f912f8aea1ad32+&cd=4&hl=en&ct=clnk&gl=uk&client=safari
//    trait DefaultsTo[Type, Default]
//
//    object DefaultsTo {
//      implicit def defaultDefaultsTo[T]: DefaultsTo[T, T] = null
//      implicit def fallback[T, D]: DefaultsTo[T, D] = null
//    }
//
//    // use target.runtimeClass in the implementation
//    def get[D](id: String)(implicit default: DefaultsTo[D, JsonDocument]): Option[D] = null
//
//    val result = get("id")
//
//    val result2 = get[User]("id")
//  }
//
//
//  test("current") {
//    type JsonDocument = Document[JsonObject]
//
//    def getAs[D](id: String, options: GetOptions = GetOptions()): Option[Document[D]] = null
//    def get(id: String, options: GetOptions = GetOptions()): Option[JsonDocument] = getAs[JsonObject](id, options)
////    def getAs[D](id: String, options: GetOptions = GetOptions()): Option[Document[D]] = get(id, options, Class[D])
//
//    val result = get("id")
//
////    val result2 = getAs[User]("id")
//
//    val result3 = getAs[User]("id")
//  }
//
//  // https://github.com/EECOLOR/shapeless-wiki/blob/master/Typelevel-patterns.md
//  test("huh") {
//    trait Converter[T] {
//      def convert(in: String): String
//    }
//    object Converter {
//      // will only exist at type level, no instance can be created
//      sealed trait UpperCase
//      sealed trait LowerCase
//
//      implicit object UpperCase extends Converter[UpperCase] {
//        def convert(in: String) = in.toUpperCase
//      }
//      implicit object LowerCase extends Converter[LowerCase] {
//        def convert(in: String) = in.toLowerCase
//      }
//    }
//
//    trait ConverterType[T]
//    object ConverterType {
//      implicit def anyConverterType[T]: ConverterType[T] = null
//      implicit object defaultConverterType extends ConverterType[Converter.UpperCase]
//    }
//
//    // ConverterType[T] acts as a magnet for type T
//    class UniversalConverter[T](implicit outputType: ConverterType[T], converter: Converter[T]) {
//      def convert(in:String) = converter convert in
//    }
//
//    val withUpperCaseConverter = new UniversalConverter
//    val withLowerCaseConverter = new UniversalConverter[Converter.LowerCase]
//  }
//
//  test("huh2") {
//    trait Converter[T] {
//      def convert(in: String): String
//    }
//    object Converter {
//      // will only exist at type level, no instance can be created
//      sealed trait UpperCase
//      sealed trait LowerCase
//
//      implicit object UpperCase extends Converter[UpperCase] {
//        def convert(in: String) = in.toUpperCase
//      }
//      implicit object LowerCase extends Converter[LowerCase] {
//        def convert(in: String) = in.toLowerCase
//      }
//    }
//
//    trait ConverterType[T]
//    object ConverterType {
//      implicit def anyConverterType[T]: ConverterType[T] = null
//      implicit object defaultConverterType extends ConverterType[Converter.UpperCase]
//    }
//
//    // ConverterType[T] acts as a magnet for type T
//    def get[T](id: String)(implicit outputType: ConverterType[T], converter: Converter[T]): Document[T] = {
//      null
//    }
//
//    val withUpperCaseConverter = get("hello")
//    val withLowerCaseConverter = get[Converter.LowerCase]("id")
//
//
//  }}
