package com.couchbase.client.scala.api

import com.couchbase.client.core.msg.kv.{SubdocCommandType, SubdocMutateRequest}
import com.couchbase.client.scala.codec.Conversions.{Encodable}
import com.couchbase.client.scala.document.EncodeParams
import io.netty.buffer.Unpooled
import io.netty.util.CharsetUtil

import scala.util.Try

sealed trait MutateOperation {
  def convert: SubdocMutateRequest.Command
  val typ: SubdocCommandType
}

trait MutateOperationSimple extends MutateOperation {
  val path: String
  val fragment: Try[(Array[Byte], EncodeParams)]
  val xattr: Boolean
  val createParent: Boolean
  val expandMacro: Boolean
  def convert = new SubdocMutateRequest.Command(typ, path, value, createParent, xattr, expandMacro)
  def value = fragment.get._1
}

case class InsertOperation(path: String, fragment: Try[(Array[Byte], EncodeParams)],
                           xattr: Boolean, createParent: Boolean, expandMacro: Boolean) extends MutateOperationSimple {
  override val typ: SubdocCommandType = SubdocCommandType.DICT_ADD
}

case class ReplaceOperation(path: String, fragment: Try[(Array[Byte], EncodeParams)],
                           xattr: Boolean, expandMacro: Boolean) extends MutateOperation {
  override val typ: SubdocCommandType = SubdocCommandType.REPLACE
  def convert = new SubdocMutateRequest.Command(typ, path, fragment.get._1, false, xattr, expandMacro)
}

case class UpsertOperation(path: String, fragment: Try[(Array[Byte], EncodeParams)],
                            xattr: Boolean, createParent: Boolean, expandMacro: Boolean) extends MutateOperationSimple {
  override val typ: SubdocCommandType = SubdocCommandType.DICT_UPSERT
}
//case class MergeOperation(path: String, value: Array[Byte], xattr: Boolean = false, createPath: Boolean = false, expandMacro: Boolean = false) extends MutateOperation
case class RemoveOperation(path: String, xattr: Boolean = false) extends MutateOperation {
  override val typ: SubdocCommandType = SubdocCommandType.DELETE
  def convert = new SubdocMutateRequest.Command(typ, path, Array[Byte](), false, xattr, false)
}
case class ArrayAppendOperation(path: String, fragment: Try[(Array[Byte], EncodeParams)],
                           xattr: Boolean, createParent: Boolean, expandMacro: Boolean) extends MutateOperationSimple {
  override val typ: SubdocCommandType = SubdocCommandType.ARRAY_PUSH_LAST
}
case class ArrayPrependOperation(path: String, fragment: Try[(Array[Byte], EncodeParams)],
                                xattr: Boolean, createParent: Boolean, expandMacro: Boolean) extends MutateOperationSimple {
  override val typ: SubdocCommandType = SubdocCommandType.ARRAY_PUSH_FIRST
}
case class ArrayInsertOperation(path: String, fragment: Try[(Array[Byte], EncodeParams)],
                                xattr: Boolean, createParent: Boolean, expandMacro: Boolean) extends MutateOperationSimple {
  override val typ: SubdocCommandType = SubdocCommandType.ARRAY_INSERT
}
case class ArrayAddUniqueOperation(path: String, fragment: Try[(Array[Byte], EncodeParams)],
                                   xattr: Boolean, createParent: Boolean, expandMacro: Boolean) extends MutateOperationSimple {
  override val typ: SubdocCommandType = SubdocCommandType.ARRAY_ADD_UNIQUE
}
case class IncrementOperation(path: String, delta: Long,
                                xattr: Boolean, createParent: Boolean) extends MutateOperation {
  override val typ: SubdocCommandType = SubdocCommandType.COUNTER
  def convert = {
    val bytes = delta.toString.getBytes(CharsetUtil.UTF_8)
    new SubdocMutateRequest.Command(typ, path, bytes, false, xattr, false)
  }
}

case class MutateInSpec(operations: List[MutateOperation]) {
  def insert[T](path: String, value: T, xattr: Boolean = false, createPath: Boolean = false, expandMacro: Boolean = false)
               (implicit ev: Encodable[T]): MutateInSpec = {
    val encoded = ev.encode(value)
    copy(operations = operations :+ InsertOperation(path, encoded, xattr, createPath, expandMacro))
  }

  def replace[T](path: String, value: T, xattr: Boolean = false, expandMacro: Boolean = false)
               (implicit ev: Encodable[T]): MutateInSpec = {
    copy(operations = operations :+ ReplaceOperation(path, ev.encode(value), xattr, expandMacro))
  }

  def upsert[T](path: String, value: T, xattr: Boolean = false, createPath: Boolean = false, expandMacro: Boolean = false)
                (implicit ev: Encodable[T]): MutateInSpec = {
    copy(operations = operations :+ UpsertOperation(path, ev.encode(value), xattr, createPath, expandMacro))
  }

  def remove(path: String, xattr: Boolean = false): MutateInSpec = {
    copy(operations = operations :+ RemoveOperation(path, xattr))
  }

  def arrayAppend[T](path: String, value: T, xattr: Boolean = false, createPath: Boolean = false, expandMacro: Boolean = false)
                (implicit ev: Encodable[T]): MutateInSpec = {
    copy(operations = operations :+ ArrayAppendOperation(path, ev.encode(value), xattr, createPath, expandMacro))
  }

  def arrayPrepend[T](path: String, value: T, xattr: Boolean = false, createPath: Boolean = false, expandMacro: Boolean = false)
                    (implicit ev: Encodable[T]): MutateInSpec = {
    copy(operations = operations :+ ArrayPrependOperation(path, ev.encode(value), xattr, createPath, expandMacro))
  }

  def arrayInsert[T](path: String, value: T, xattr: Boolean = false, createPath: Boolean = false, expandMacro: Boolean = false)
                    (implicit ev: Encodable[T]): MutateInSpec = {
    copy(operations = operations :+ ArrayInsertOperation(path, ev.encode(value), xattr, createPath, expandMacro))
  }

  def arrayAddUnique[T](path: String, value: T, xattr: Boolean = false, createPath: Boolean = false, expandMacro: Boolean = false)
                       (implicit ev: Encodable[T]): MutateInSpec = {
    copy(operations = operations :+ ArrayAddUniqueOperation(path, ev.encode(value), xattr, createPath, expandMacro))
  }

  def increment(path: String, delta: Long, xattr: Boolean = false, createPath: Boolean = false): MutateInSpec = {
    copy(operations = operations :+ IncrementOperation(path, delta, xattr, createPath))
  }

  def decrement(path: String, delta: Long, xattr: Boolean = false, createPath: Boolean = false): MutateInSpec = {
    copy(operations = operations :+ IncrementOperation(path, delta * -1, xattr, createPath))
  }
}

object MutateInSpec {
  def apply() = new MutateInSpec(List.empty)

  val empty = MutateInSpec(List())

  def insert[T](path: String, value: T, xattr: Boolean = false, createPath: Boolean = false, expandMacro: Boolean = false)
               (implicit ev: Encodable[T]): MutateInSpec = {
    empty.insert(path, value, xattr, createPath, expandMacro)
  }

  def replace[T](path: String, value: T, xattr: Boolean = false, expandMacro: Boolean = false)
               (implicit ev: Encodable[T]): MutateInSpec = {
    empty.replace(path, value, xattr, expandMacro)
  }

  def upsert[T](path: String, value: T, xattr: Boolean = false, createPath: Boolean = false, expandMacro: Boolean = false)
                (implicit ev: Encodable[T]): MutateInSpec = {
    empty.upsert(path, value, xattr, createPath, expandMacro)
  }

  def remove(path: String, xattr: Boolean = false): MutateInSpec = {
    empty.remove(path, xattr)
  }

  def arrayAppend[T](path: String, value: T, xattr: Boolean = false, createPath: Boolean = false, expandMacro: Boolean = false)
               (implicit ev: Encodable[T]): MutateInSpec = {
    empty.arrayAppend(path, value, xattr, createPath, expandMacro)
  }

  def arrayPrepend[T](path: String, value: T, xattr: Boolean = false, createPath: Boolean = false, expandMacro: Boolean = false)
                    (implicit ev: Encodable[T]): MutateInSpec = {
    empty.arrayPrepend(path, value, xattr, createPath, expandMacro)
  }

  def arrayInsert[T](path: String, value: T, xattr: Boolean = false, createPath: Boolean = false, expandMacro: Boolean = false)
                    (implicit ev: Encodable[T]): MutateInSpec = {
    empty.arrayInsert(path, value, xattr, createPath, expandMacro)
  }

  def arrayAddUnique[T](path: String, value: T, xattr: Boolean = false, createPath: Boolean = false, expandMacro: Boolean = false)
                       (implicit ev: Encodable[T]): MutateInSpec = {
    empty.arrayAddUnique(path, value, xattr, createPath, expandMacro)
  }

  def increment(path: String, delta: Long, xattr: Boolean = false, createPath: Boolean = false): MutateInSpec = {
    empty.increment(path, delta, xattr, createPath)
  }

  def decrement(path: String, delta: Long, xattr: Boolean = false, createPath: Boolean = false): MutateInSpec = {
    empty.decrement(path, delta, xattr, createPath)
  }
}