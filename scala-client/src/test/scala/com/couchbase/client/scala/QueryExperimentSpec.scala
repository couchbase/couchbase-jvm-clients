package com.couchbase.client.scala

import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.JsonObject
import org.scalatest.{FlatSpec, FunSuite}

import scala.reflect.ClassTag
import scala.language.dynamics

object Select {
  def star() = SelectParams("*")
  def *() = SelectParams("*")
}
case class SelectParams(params: String) {
//  def where() = new WhereClause()
  def from(bucket: String) = new FromClause(bucket)
}
case class FromClause(bucket: String) {
  def where(clause: String) = WhereClause(clause)
  def where = WhereClause("")
}
case class WhereClause(clause: String) extends Dynamic {
  def applyDynamic(v: String): WhereSubClause = WhereSubClause(v)
  def updateDynamic(v: String): WhereSubClause = WhereSubClause(v)
}
case class WhereSubClause(v: String)

trait N1QL {
  def select = Select
}


class GetExperimentSpec extends FunSuite with N1QL  {

  test("dsl") {
//    val query = select *() from "default" where "age > 5"
//    val query = (select *() from "default" where).applyDynamic("age") = 5
//    val query = (select *() from "default" where).age = 5
//    val query = select *() from "default" where age = 5

//    val query select.*().from("default").where age = 5

  }

  test("java") {
    com.couchbase.client.java.query.Select.select("*").from("default").where("age > 5")
  }
}
