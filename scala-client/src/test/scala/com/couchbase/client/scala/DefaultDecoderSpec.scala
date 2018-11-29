package com.couchbase.client.scala

import java.lang.reflect.{ParameterizedType, Type}
import java.nio.charset.Charset

import com.couchbase.client.scala.document.DefaultDecoder
import com.couchbase.client.scala.document.DefaultDecoder.mapper
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.scalatest.FunSuite
import upickle.default._
import upickle._
import io.circe._, io.circe.generic.auto._, io.circe.parser._, io.circe.syntax._

sealed trait Decode

class DefaultDecoderSpec extends FunSuite {
  private val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)

//  def decode[T](in: Array[Byte]): T = DefaultDecoder.decode[T](in)
//  def decode[T](in: String): T = DefaultDecoder.decode[T](in)

//  test("1") {
//    val user = User("john", 42)
//    val json = mapper.writeValueAsString(user)
//    val bytes = json.getBytes(Charset.defaultCharset())
//    val decodedFromString = read[User](json)
////    val decodedFromString = DefaultDecoder.decode[User](json)
////    val decodedFromBytes = DefaultDecoder.decode[User](bytes)
//    assert(user == decodedFromString)
////    assert(user == decodedFromBytes)
//  }

  test("User1") {
    case class User(name: String, age: Int) extends Decode

    val user = User("john", 42)
    val json = user.asJson.noSpaces
    val decoded = decode[User](json)
    decoded match {
      case Left(x) => fail()
      case Right(x) => assert(user == x)
    }


  }

  test("User2") {
    case class User(name: String, age: Option[Int]) extends Decode

    val user = User("john", Some(42))
    val json = user.asJson.noSpaces
    val decoded = decode[User](json)
    decoded match {
      case Left(x) => fail()
      case Right(x) => assert(user == x)
    }


  }

  test("User3") {
    case class User(name: String, age: Option[Int]) extends Decode

    val user = User("john", None)
    val json = user.asJson.noSpaces
    val decoded = decode[User](json)
    decoded match {
      case Left(x) => fail()
      case Right(x) => assert(user == x)
    }
  }
}
