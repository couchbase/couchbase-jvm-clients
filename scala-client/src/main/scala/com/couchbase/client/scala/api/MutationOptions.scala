package com.couchbase.client.scala.api

import com.couchbase.client.core.msg.kv.{SubdocCommandType, SubdocMutateRequest}
import com.couchbase.client.scala.document.Conversions.{Encodable, EncodableField}
import com.couchbase.client.scala.document.{EncodeParams, JsonObject}

import scala.util.Try

sealed trait MutateOperation {
  val fragment: Try[(Array[Byte], EncodeParams)]
  def value = fragment.get._1
  def convert: SubdocMutateRequest.Command
}

trait MutateOperationSimple extends MutateOperation {
  val typ: SubdocCommandType
  val path: String
  val fragment: Try[(Array[Byte], EncodeParams)]
  val xattrs: Boolean
  val createParent: Boolean
  val expandMacros: Boolean
  def convert = new SubdocMutateRequest.Command(typ, path, value, createParent, xattrs)
}

case class InsertOperation(path: String, fragment: Try[(Array[Byte], EncodeParams)],
                           xattrs: Boolean, createParent: Boolean, expandMacros: Boolean) extends MutateOperationSimple {
  override val typ: SubdocCommandType = SubdocCommandType.DICT_ADD
}

case class ReplaceOperation(path: String, fragment: Try[(Array[Byte], EncodeParams)],
                           xattrs: Boolean, createParent: Boolean, expandMacros: Boolean) extends MutateOperationSimple {
  override val typ: SubdocCommandType = SubdocCommandType.REPLACE
}

case class UpsertOperation(path: String, fragment: Try[(Array[Byte], EncodeParams)],
                            xattrs: Boolean, createParent: Boolean, expandMacros: Boolean) extends MutateOperationSimple {
  override val typ: SubdocCommandType = SubdocCommandType.DICT_UPSERT
}
//case class ReplaceOperation(path: String, value: Array[Byte], xattrs: Boolean = false, createPath: Boolean = false, expandMacros: Boolean = false) extends MutateOperation
//case class UpsertOperation(path: String, value: Array[Byte], xattrs: Boolean = false, createPath: Boolean = false, expandMacros: Boolean = false) extends MutateOperation
//case class MergeOperation(path: String, value: Array[Byte], xattrs: Boolean = false, createPath: Boolean = false, expandMacros: Boolean = false) extends MutateOperation
//case class RemoveOperation(path: String, xattrs: Boolean = false, createPath: Boolean = false, expandMacros: Boolean = false) extends MutateOperation
//case class CounterOperation(path: String, delta: Long, xattrs: Boolean = false, createPath: Boolean = false, expandMacros: Boolean = false) extends MutateOperation
//case class ArrayPrependOperation(path: String, value: Array[Byte], xattrs: Boolean = false, createPath: Boolean = false, expandMacros: Boolean = false) extends MutateOperation
// TODO other array ops
//case class ContentOperation(content: JsonObject) extends MutateOperation
//case class MutateOptions(xattrs: Boolean = false, expandMacros: Boolean = false, createPath: Boolean = false)

case class MutateInSpec(operations: List[MutateOperation]) {
  def insert[T](path: String, value: T, xattrs: Boolean = false, createPath: Boolean = false, expandMacros: Boolean = false)
               (implicit ev: EncodableField[T]): MutateInSpec = {
    val encoded = ev.encode(value)
    copy(operations = operations :+ InsertOperation(path, encoded, xattrs, createPath, expandMacros))
  }

  def replace[T](path: String, value: T, xattrs: Boolean = false, createPath: Boolean = false, expandMacros: Boolean = false)
               (implicit ev: EncodableField[T]): MutateInSpec = {
    copy(operations = operations :+ ReplaceOperation(path, ev.encode(value), xattrs, createPath, expandMacros))
  }

  def upsert[T](path: String, value: T, xattrs: Boolean = false, createPath: Boolean = false, expandMacros: Boolean = false)
                (implicit ev: EncodableField[T]): MutateInSpec = {
    copy(operations = operations :+ UpsertOperation(path, ev.encode(value), xattrs, createPath, expandMacros))
  }

  //  def replace(path: String, value: Any, xattrs: Boolean = false, createPath: Boolean = false, expandMacros: Boolean = false): MutateInOps = {
//    copy(operations = operations :+ ReplaceOperation(path, value, xattrs, createPath, expandMacros))
//  }
//
//  // Given `case class(name: String, age: Int)`, merge will upsert only fields name and age
//  def upsert(path: String, value: Any, xattrs: Boolean = false, createPath: Boolean = false, expandMacros: Boolean = false): MutateInOps = {
//    copy(operations = operations :+ UpsertOperation(path, value, xattrs, createPath, expandMacros))
//  }
//
//  def mergeUpsert(path: String, value: Any, xattrs: Boolean = false, createPath: Boolean = false, expandMacros: Boolean = false): MutateInOps = {
//    copy(operations = operations :+ MergeOperation(path, value, xattrs, createPath, expandMacros))
//  }
//
//  def remove(path: String, xattrs: Boolean = false, createPath: Boolean = false, expandMacros: Boolean = false): MutateInOps = {
//    copy(operations = operations :+ RemoveOperation(path, xattrs, createPath, expandMacros))
//  }
//
//  def counter(path: String, delta: Long, xattrs: Boolean = false, createPath: Boolean = false, expandMacros: Boolean = false): MutateInOps = {
//    copy(operations = operations :+ CounterOperation(path, delta, xattrs, createPath, expandMacros))
//  }
//
//  def arrayPrepend(path: String, value: Any, xattrs: Boolean = false, createPath: Boolean = false, expandMacros: Boolean = false): MutateInOps = {
//    copy(operations = operations :+ ArrayPrependOperation(path, value, xattrs, createPath, expandMacros))
//  }
//
//  def content(content: JsonObject): MutateInOps = {
//    copy(operations = operations :+ ContentOperation(content))
//  }

}

object MutateInSpec {
  def apply() = new MutateInSpec(List.empty)

  val empty = MutateInSpec(List())

  def insert[T](path: String, value: T, xattrs: Boolean = false, createPath: Boolean = false, expandMacros: Boolean = false)
               (implicit ev: EncodableField[T]): MutateInSpec = {
    empty.insert(path, value, xattrs, createPath, expandMacros)
  }

  def replace[T](path: String, value: T, xattrs: Boolean = false, createPath: Boolean = false, expandMacros: Boolean = false)
               (implicit ev: EncodableField[T]): MutateInSpec = {
    empty.replace(path, value, xattrs, createPath, expandMacros)
  }

  def upsert[T](path: String, value: T, xattrs: Boolean = false, createPath: Boolean = false, expandMacros: Boolean = false)
                (implicit ev: EncodableField[T]): MutateInSpec = {
    empty.upsert(path, value, xattrs, createPath, expandMacros)
  }
}