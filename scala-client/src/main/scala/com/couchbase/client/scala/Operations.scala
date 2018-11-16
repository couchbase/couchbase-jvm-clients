package com.couchbase.client.scala

import scala.language.dynamics

sealed trait Operation
case class GetOperation(path: String, xattr: Boolean) extends Operation
case class GetStringOperation(path: String, xattr: Boolean) extends Operation
case class GetIntOperation(path: String, xattr: Boolean) extends Operation
case class ExistsOperation(path: String, xattr: Boolean) extends Operation

case class GetFields(operations: List[Operation]) {
  def get(path: String, xattr: Boolean = false): GetFields = {
    copy(operations = operations :+ GetOperation(path, xattr))
  }

  def getString(path: String, xattr: Boolean = false): GetFields = {
    copy(operations = operations :+ GetStringOperation(path, xattr))
  }

  def getInt(path: String, xattr: Boolean = false): GetFields = {
    copy(operations = operations :+ GetIntOperation(path, xattr))
  }

  def exists(path: String, xattr: Boolean = false): GetFields = {
    copy(operations = operations :+ ExistsOperation(path, xattr))
  }
}

object GetFields {
  def apply() = new GetFields(List.empty[Operation])
}

class FieldsResult extends Dynamic {
  def content(idx: Int): Any = null
  def content(idx: String): Any = null

  def selectDynamic(name: String): Any = content(name)
}