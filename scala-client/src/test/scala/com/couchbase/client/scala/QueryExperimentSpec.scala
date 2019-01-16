package com.couchbase.client.scala

import com.couchbase.client.scala.query.{N1qlQueryResult, N1qlRow}
import org.scalatest.{FlatSpec, FunSuite}

import scala.reflect.ClassTag
import scala.language.dynamics
import scala.language.experimental.macros

object Select {
  def star() = SelectParams("*")
  def *() = SelectParams("*")
  def applyDynamic(key: String) = SelectParams("")
}
case class SelectParams(params: Any*) {
  //  def where() = new WhereClause()

  def *() = SelectParams("*")

  def from(bucket: String) = new FromClause(bucket)
  def FROM(bucket: String) = new FromClause(bucket)
  def from(bucket: Bucket) = new FromClause("bucket")
  def FROM(bucket: Bucket) = new FromClause("bucket")

  def applyDynamic(name: String)(args: Any*) = SelectParams("")

  def as(name: String) = SelectAs(name, this)

  def AS(name: String) = SelectAs(name, this)

  override def toString: String = if (params.size == 1) {
    "select " + params.head + " "
  }
  else {
    "select " + params.mkString(", ") + " "
  }
}
case class SelectAs(name: String, params: SelectParams) {
  override def toString: String = params.toString + " as \"" + name + '"'

  def from(bucket: String) = new FromClause(bucket)
  def FROM(bucket: String) = new FromClause(bucket)
  def from(bucket: Bucket) = new FromClause("bucket")
  def FROM(bucket: Bucket) = new FromClause("bucket")

}
case class FromClause(bucket: String) {
//  def where(clause: String) = WhereClause(clause)
//  def where = WhereClause("")
  def where(e: Expression) = WhereClause("")
  def WHERE(e: Expression) = WhereClause("")
  def WHERE(e: (ExpressionBuilder) => Expression) = e()
  override def toString: String = "select "
}
case class ExpressionBuilder() extends Dynamic {
//  def updateDynamic(key: String)(v: Any): Expression = Expression()
  def selectDynamic(key: String) = ExprPart(key)
  def build: Expression = ???

}
case class ExprPart(name: String) {
  def eqls(v: Any) = Expression()
  def >(v: Any) = Expression()
  def <(v: Any) = Expression()
}
case class WhereClause(clause: String) extends Dynamic {
//  def applyDynamic(v: String): WhereSubClause = WhereSubClause(v)
//  def selectDynamic(key: String) = null
//def updateDynamic(key: String)(v: Any): WhereSubClause = WhereSubClause(v)
  def and(e: Expression): WhereClause = ???
  def or(e: Expression): WhereClause = ???
}
case class WhereSubClause(v: Any) extends Dynamic {
  def and: WhereSubClause = WhereSubClause()
  def selectDynamic(key: String) = null
  def updateDynamic(key: String)(v: Any): WhereSubClause = WhereSubClause(v)
}

trait N1QL {
  def select = Select
}


case class Expression()



case class SelectExpr(value: Any)

class DslExperimentSpec extends FunSuite with N1QL  {
  implicit class PimpString(v: String) {
    def >(arg: Any): Expression = ???
    def eqls(arg: String): Expression = ???
  }

  implicit def Any2SelectExpr(value : Any) =
    new SelectExpr(value)


  implicit object Query {
    def select(args: Any*) = SelectParams(args)
    def SELECT(args: SelectExpr*) = SelectParams(args)
    def SELECT = SelectParams()
  }

  val tutorial: Bucket = ???


  test("java") {
//    com.couchbase.client.java.query.Select.select("*").from("default").where("age > 5")
  }

  /*
   Note: aborting this experiment.  Scala just isn't dynamic enough to be able to support full SQL syntax.
   */

  test("""SELECT 'Hello World' AS Greeting""") {
    val query = Query.SELECT ("Hello World") AS "Greeting"
    println(query.toString)
  }

  test("""SELECT *
         |  FROM tutorial
         |    WHERE fname = 'Ian'""".stripMargin) {
    val x = Query.SELECT *
    val query = Query.SELECT ("*") FROM "tutorial" WHERE ("fname" eqls "Ian")
    println(query.toString)
  }

  test("""SELECT children[0].fname AS child_name
         |    FROM tutorial
         |       WHERE fname='Dave'""") {
    val query = Query.SELECT ("children[0].fname") AS "child_name" FROM tutorial WHERE ("fname" eqls "Dave")
    val query2 = Query.SELECT ("children[0].fname") AS "child_name" FROM tutorial WHERE (_.fname eqls "Dave")


  }
}
