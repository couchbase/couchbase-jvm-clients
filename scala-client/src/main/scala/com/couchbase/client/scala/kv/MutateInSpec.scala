package com.couchbase.client.scala.kv

import com.couchbase.client.core.msg.kv.{SubdocCommandType, SubdocMutateRequest}
import com.couchbase.client.scala.codec.Conversions.Encodable
import com.couchbase.client.scala.codec.EncodeParams
import io.netty.util.CharsetUtil

import scala.util.Try

sealed trait MutateInSpec {
  def convert: SubdocMutateRequest.Command
  val typ: SubdocCommandType
}

private trait MutateInSpecStandard extends MutateInSpec {
  val path: String
  val fragment: Try[(Array[Byte], EncodeParams)]
  val xattr: Boolean
  val createParent: Boolean
  val expandMacro: Boolean
  def convert = new SubdocMutateRequest.Command(typ, path, value, createParent, xattr, expandMacro)
  def value = fragment.get._1
}

private case class Insert(path: String, fragment: Try[(Array[Byte], EncodeParams)],
                  xattr: Boolean, createParent: Boolean, expandMacro: Boolean) extends MutateInSpecStandard {
  override val typ: SubdocCommandType = SubdocCommandType.DICT_ADD
}

private case class Replace(path: String, fragment: Try[(Array[Byte], EncodeParams)],
                   xattr: Boolean, expandMacro: Boolean) extends MutateInSpec {
  override val typ: SubdocCommandType = SubdocCommandType.REPLACE
  def convert = new SubdocMutateRequest.Command(typ, path, fragment.get._1, false, xattr, expandMacro)
}

private case class Upsert(path: String, fragment: Try[(Array[Byte], EncodeParams)],
                  xattr: Boolean, createParent: Boolean, expandMacro: Boolean) extends MutateInSpecStandard {
  override val typ: SubdocCommandType = SubdocCommandType.DICT_UPSERT
}
private case class Remove(path: String, xattr: Boolean = false) extends MutateInSpec {
  override val typ: SubdocCommandType = SubdocCommandType.DELETE
  def convert = new SubdocMutateRequest.Command(typ, path, Array[Byte](), false, xattr, false)
}
private case class ArrayAppend(path: String, fragment: Try[(Array[Byte], EncodeParams)],
                       xattr: Boolean, createParent: Boolean, expandMacro: Boolean) extends MutateInSpecStandard {
  override val typ: SubdocCommandType = SubdocCommandType.ARRAY_PUSH_LAST
}
private case class ArrayPrepend(path: String, fragment: Try[(Array[Byte], EncodeParams)],
                        xattr: Boolean, createParent: Boolean, expandMacro: Boolean) extends MutateInSpecStandard {
  override val typ: SubdocCommandType = SubdocCommandType.ARRAY_PUSH_FIRST
}
private case class ArrayInsert(path: String, fragment: Try[(Array[Byte], EncodeParams)],
                       xattr: Boolean, createParent: Boolean, expandMacro: Boolean) extends MutateInSpecStandard {
  override val typ: SubdocCommandType = SubdocCommandType.ARRAY_INSERT
}
private case class ArrayAddUnique(path: String, fragment: Try[(Array[Byte], EncodeParams)],
                          xattr: Boolean, createParent: Boolean, expandMacro: Boolean) extends MutateInSpecStandard {
  override val typ: SubdocCommandType = SubdocCommandType.ARRAY_ADD_UNIQUE
}
private case class Increment(path: String, delta: Long,
                     xattr: Boolean, createParent: Boolean) extends MutateInSpec {
  override val typ: SubdocCommandType = SubdocCommandType.COUNTER
  def convert = {
    val bytes = delta.toString.getBytes(CharsetUtil.UTF_8)
    new SubdocMutateRequest.Command(typ, path, bytes, false, xattr, false)
  }
}


object MutateInSpec {
  def insert[T](path: String, value: T, xattr: Boolean = false, createPath: Boolean = false, expandMacro: Boolean = false)
               (implicit ev: Encodable[T]): MutateInSpec = {
    val encoded = ev.encodeSubDocumentField(value)
    Insert(path, encoded, xattr, createPath, expandMacro)
  }

  def replace[T](path: String, value: T, xattr: Boolean = false, expandMacro: Boolean = false)
                (implicit ev: Encodable[T]): MutateInSpec = {
    Replace(path, ev.encodeSubDocumentField(value), xattr, expandMacro)
  }

  def upsert[T](path: String, value: T, xattr: Boolean = false, createPath: Boolean = false, expandMacro: Boolean = false)
               (implicit ev: Encodable[T]): MutateInSpec = {
    Upsert(path, ev.encodeSubDocumentField(value), xattr, createPath, expandMacro)
  }

  def remove(path: String, xattr: Boolean = false): MutateInSpec = {
    Remove(path, xattr)
  }

  def arrayAppend[T](path: String, value: T, xattr: Boolean = false, createPath: Boolean = false, expandMacro: Boolean = false)
                    (implicit ev: Encodable[T]): MutateInSpec = {
    ArrayAppend(path, ev.encodeSubDocumentField(value), xattr, createPath, expandMacro)
  }

  def arrayPrepend[T](path: String, value: T, xattr: Boolean = false, createPath: Boolean = false, expandMacro: Boolean = false)
                     (implicit ev: Encodable[T]): MutateInSpec = {
    ArrayPrepend(path, ev.encodeSubDocumentField(value), xattr, createPath, expandMacro)
  }

  def arrayInsert[T](path: String, value: T, xattr: Boolean = false, createPath: Boolean = false, expandMacro: Boolean = false)
                    (implicit ev: Encodable[T]): MutateInSpec = {
    ArrayInsert(path, ev.encodeSubDocumentField(value), xattr, createPath, expandMacro)
  }

  def arrayAddUnique[T](path: String, value: T, xattr: Boolean = false, createPath: Boolean = false, expandMacro: Boolean = false)
                       (implicit ev: Encodable[T]): MutateInSpec = {
    ArrayAddUnique(path, ev.encodeSubDocumentField(value), xattr, createPath, expandMacro)
  }

  def increment(path: String, delta: Long, xattr: Boolean = false, createPath: Boolean = false): MutateInSpec = {
    Increment(path, delta, xattr, createPath)
  }

  def decrement(path: String, delta: Long, xattr: Boolean = false, createPath: Boolean = false): MutateInSpec = {
    Increment(path, delta * -1, xattr, createPath)
  }
}