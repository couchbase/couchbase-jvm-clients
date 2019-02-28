package com.couchbase.client.scala

import com.couchbase.client.core.error.DocumentDoesNotExistException
import org.scalatest.FunSuite

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Failure, Success}

class BinarySpec extends FunSuite {

    val cluster = Cluster.connect("localhost", "Administrator", "password")
    val bucket = cluster.bucket("default")
    val coll = bucket.defaultCollection.binary


  private val reactive = coll.reactive
  private val async = coll.async

  test("blocking increment") {
    val docId = TestUtils.docId()
    coll.increment(docId, 3, Option(0)) match {
      case Success(result) => assert(result.content == 0) // initial value returned
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  test("blocking increment exists") {
    val docId = TestUtils.docId()
    coll.increment(docId, 0, Option(0))
    coll.increment(docId, 5, Option(999)) match {
      case Success(result) => assert(result.content == 5) // new value value returned
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  test("blocking increment exists no initial") {
    val docId = TestUtils.docId()
    coll.increment(docId, 0, Option(0))
    coll.increment(docId, 5) match {
      case Success(result) => assert(result.content == 5) // new value value returned
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  test("blocking increment no initial") {
    val docId = TestUtils.docId()
    coll.increment(docId, 3) match {
      case Success(result) => assert(false, s"success not expected")
      case Failure(err: DocumentDoesNotExistException) => 
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }


  test("blocking decrement") {
    val docId = TestUtils.docId()
    coll.decrement(docId, 3, Option(0)) match {
      case Success(result) => assert(result.content == 0) // initial value returned
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  test("blocking decrement exists at 0") {
    val docId = TestUtils.docId()
    coll.decrement(docId, 0, Option(0))
    coll.decrement(docId, 5, Option(999)) match {
      case Success(result) => assert(result.content == 0) // remember decrement won't go below 0
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  test("blocking decrement exists at 100") {
    val docId = TestUtils.docId()
    coll.decrement(docId, 0, Option(100))
    coll.decrement(docId, 5, Option(999)) match {
      case Success(result) => assert(result.content == 95)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  test("blocking decrement exists no initial") {
    val docId = TestUtils.docId()
    coll.decrement(docId, 0, Option(0))
    coll.decrement(docId, 5) match {
      case Success(result) => assert(result.content == 0)  // remember decrement won't go below 0
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  test("blocking decrement no initial") {
    val docId = TestUtils.docId()
    coll.decrement(docId, 3) match {
      case Success(result) => assert(false, s"success not expected")
      case Failure(err: DocumentDoesNotExistException) =>
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }


}
