package com.couchbase.client.scala.api

import com.couchbase.client.scala.document.JsonObject

sealed trait MutateOperation
case class InsertOperation(path: String, value: Any, options: MutateOptions) extends MutateOperation
case class ReplaceOperation(path: String, value: Any, options: MutateOptions) extends MutateOperation
case class UpsertOperation(path: String, value: Any, options: MutateOptions) extends MutateOperation
case class RemoveOperation(path: String, options: MutateOptions) extends MutateOperation
case class CounterOperation(path: String, delta: Long, options: MutateOptions) extends MutateOperation
case class ArrayPrependOperation(path: String, value: Any, options: MutateOptions) extends MutateOperation
// TODO other array ops
case class ContentOperation(content: JsonObject) extends MutateOperation
case class MutateOptions(xattrs: Boolean = false, expandMacros: Boolean = false, createPath: Boolean = false)

case class MutateFields(operations: List[MutateOperation]) {
  def insert(path: String, value: Any, options: MutateOptions = MutateOptions()): MutateFields = {
    copy(operations = operations :+ InsertOperation(path, value, options))
  }

  def replace(path: String, value: Any, options: MutateOptions = MutateOptions()): MutateFields = {
    copy(operations = operations :+ ReplaceOperation(path, value, options))
  }

  def upsert(path: String, value: Any, options: MutateOptions = MutateOptions()): MutateFields = {
    copy(operations = operations :+ UpsertOperation(path, value, options))
  }

  def remove(path: String, options: MutateOptions = MutateOptions()): MutateFields = {
    copy(operations = operations :+ RemoveOperation(path, options))
  }

  def counter(path: String, delta: Long, options: MutateOptions = MutateOptions()): MutateFields = {
    copy(operations = operations :+ CounterOperation(path, delta, options))
  }

  def arrayPrepend(path: String, value: Any, options: MutateOptions = MutateOptions()): MutateFields = {
    copy(operations = operations :+ ArrayPrependOperation(path, value, options))
  }

  def content(content: JsonObject): MutateFields = {
    copy(operations = operations :+ ContentOperation(content))
  }

}
