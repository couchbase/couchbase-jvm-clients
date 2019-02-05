package com.couchbase.client.scala.api

import com.couchbase.client.core.msg.kv.{SubdocCommandType, SubdocMutateRequest}
import com.couchbase.client.scala.document.Conversions.{Encodable, EncodableField}
import com.couchbase.client.scala.document.{EncodeParams}

import scala.util.Try

sealed trait MutateOperation {
  def convert: SubdocMutateRequest.Command
  val typ: SubdocCommandType
}

trait MutateOperationSimple extends MutateOperation {
  val path: String
  val fragment: Try[(Array[Byte], EncodeParams)]
  val xattrs: Boolean
  val createParent: Boolean
  val expandMacros: Boolean
  def convert = new SubdocMutateRequest.Command(typ, path, value, createParent, xattrs)
  def value = fragment.get._1
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
//case class MergeOperation(path: String, value: Array[Byte], xattrs: Boolean = false, createPath: Boolean = false, expandMacros: Boolean = false) extends MutateOperation
case class RemoveOperation(path: String, xattrs: Boolean = false, createPath: Boolean = false, expandMacros: Boolean = false) extends MutateOperation {
  override val typ: SubdocCommandType = SubdocCommandType.DELETE
  def convert = new SubdocMutateRequest.Command(typ, path, null, false, xattrs)
}
case class ArrayAppendOperation(path: String, fragment: Try[(Array[Byte], EncodeParams)],
                           xattrs: Boolean, createParent: Boolean, expandMacros: Boolean) extends MutateOperationSimple {
  override val typ: SubdocCommandType = SubdocCommandType.ARRAY_PUSH_LAST
}
case class ArrayPrependOperation(path: String, fragment: Try[(Array[Byte], EncodeParams)],
                                xattrs: Boolean, createParent: Boolean, expandMacros: Boolean) extends MutateOperationSimple {
  override val typ: SubdocCommandType = SubdocCommandType.ARRAY_PUSH_FIRST
}
case class ArrayInsertOperation(path: String, fragment: Try[(Array[Byte], EncodeParams)],
                                xattrs: Boolean, createParent: Boolean, expandMacros: Boolean) extends MutateOperationSimple {
  override val typ: SubdocCommandType = SubdocCommandType.ARRAY_INSERT
}
case class ArrayAddUniqueOperation(path: String, fragment: Try[(Array[Byte], EncodeParams)],
                                   xattrs: Boolean, createParent: Boolean, expandMacros: Boolean) extends MutateOperationSimple {
  override val typ: SubdocCommandType = SubdocCommandType.ARRAY_ADD_UNIQUE
}
// TODO increment
//case class IncrementOperation(path: String, delta: Long,
//                                xattrs: Boolean, createParent: Boolean) extends MutateOperation {
//  override val typ: SubdocCommandType = SubdocCommandType.COUNTER
//  def convert = new SubdocMutateRequest.Command(typ, path, Array(delta.toByte), false, xattrs)
//}
// TODO decrement

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

  def remove(path: String, xattrs: Boolean = false): MutateInSpec = {
    copy(operations = operations :+ RemoveOperation(path, xattrs))
  }

  def arrayAppend[T](path: String, value: T, xattrs: Boolean = false, createPath: Boolean = false, expandMacros: Boolean = false)
                (implicit ev: EncodableField[T]): MutateInSpec = {
    copy(operations = operations :+ ArrayAppendOperation(path, ev.encode(value), xattrs, createPath, expandMacros))
  }

  def arrayPrepend[T](path: String, value: T, xattrs: Boolean = false, createPath: Boolean = false, expandMacros: Boolean = false)
                    (implicit ev: EncodableField[T]): MutateInSpec = {
    copy(operations = operations :+ ArrayPrependOperation(path, ev.encode(value), xattrs, createPath, expandMacros))
  }

  def arrayInsert[T](path: String, value: T, xattrs: Boolean = false, createPath: Boolean = false, expandMacros: Boolean = false)
                    (implicit ev: EncodableField[T]): MutateInSpec = {
    copy(operations = operations :+ ArrayInsertOperation(path, ev.encode(value), xattrs, createPath, expandMacros))
  }

  def arrayAddUnique[T](path: String, value: T, xattrs: Boolean = false, createPath: Boolean = false, expandMacros: Boolean = false)
                       (implicit ev: EncodableField[T]): MutateInSpec = {
    copy(operations = operations :+ ArrayAddUniqueOperation(path, ev.encode(value), xattrs, createPath, expandMacros))
  }

//  def increment(path: String, delta: Long, xattrs: Boolean = false, createPath: Boolean = false): MutateInSpec = {
//    copy(operations = operations :+ IncrementOperation(path, delta, xattrs, createPath))
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

  def remove(path: String, xattrs: Boolean = false): MutateInSpec = {
    empty.remove(path, xattrs)
  }

  def arrayAppend[T](path: String, value: T, xattrs: Boolean = false, createPath: Boolean = false, expandMacros: Boolean = false)
               (implicit ev: EncodableField[T]): MutateInSpec = {
    empty.arrayAppend(path, value, xattrs, createPath, expandMacros)
  }

  def arrayPrepend[T](path: String, value: T, xattrs: Boolean = false, createPath: Boolean = false, expandMacros: Boolean = false)
                    (implicit ev: EncodableField[T]): MutateInSpec = {
    empty.arrayPrepend(path, value, xattrs, createPath, expandMacros)
  }

  def arrayInsert[T](path: String, value: T, xattrs: Boolean = false, createPath: Boolean = false, expandMacros: Boolean = false)
                    (implicit ev: EncodableField[T]): MutateInSpec = {
    empty.arrayInsert(path, value, xattrs, createPath, expandMacros)
  }

  def arrayAddUnique[T](path: String, value: T, xattrs: Boolean = false, createPath: Boolean = false, expandMacros: Boolean = false)
                       (implicit ev: EncodableField[T]): MutateInSpec = {
    empty.arrayAddUnique(path, value, xattrs, createPath, expandMacros)
  }

//  def increment(path: String, delta: Long, xattrs: Boolean = false, createPath: Boolean = false): MutateInSpec = {
//    empty.increment(path, delta, xattrs, createPath)
//  }
}