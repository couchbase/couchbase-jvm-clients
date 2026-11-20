package com.couchbase.client.scala.query.conversational

/** The type of statement the query service should generate from a natural-language prompt. */
sealed trait ConversationalQueryOutput

object ConversationalQueryOutput {

  /** Generate a SQL++ statement. */
  case object Sqlpp extends ConversationalQueryOutput

  /** Generate a SQL++ statement that uses Full-Text Search. */
  case object FtsSqlpp extends ConversationalQueryOutput

  /** Generate a JavaScript user-defined function.
    *
    * Creating the function requires a separate, ordinary query request.
    */
  case object JsUdf extends ConversationalQueryOutput
}
