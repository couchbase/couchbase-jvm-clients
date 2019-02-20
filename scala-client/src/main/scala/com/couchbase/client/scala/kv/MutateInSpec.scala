package com.couchbase.client.scala.kv

import com.couchbase.client.core.msg.kv.{SubdocCommandType, SubdocMutateRequest}
import com.couchbase.client.scala.codec.Conversions.Encodable
import com.couchbase.client.scala.codec.EncodeParams
import io.netty.util.CharsetUtil

import scala.util.Try

/** Represents an intent to perform a single SubDocument mutation. */
sealed trait MutateInSpec {
  private[scala] def convert: SubdocMutateRequest.Command

  private[scala] val typ: SubdocCommandType
}

/** Most SubDocument mutations are pretty similar, encapsulate the similarities here. */
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

// TODO change expandMacro to sentinel values


/** Methods to allow constructing a sequence of `MutateInSpec`s.
  *
  * @define CreatePath     whether intermediate paths should be created
  * @define Xattr          whether this is an extended attribute (xattr) field
  * @define SupportedTypes This can be of any type for which an implicit Encodable can be found: a list
  *                        of types that are supported 'out of the box' is available at ***CHANGEME:TYPES***
  * @define Encodable      an implicit Encodable.  For any supported type T this will be found automatically.
  */
object MutateInSpec {
  // TODO fix ***CHANGEME*** when docs are available

  /** Returns a `MutateInSpec` with the intent of inserting a value into a JSON object.
    *
    * Will error if the last element of the path already exists.
    *
    * @param path       the path identifying where to insert the value.
    * @param value      the value to insert.  $SupportedTypes
    * @param xattr      $Xattr
    * @param createPath $CreatePath
    * @param expandMacro
    * @param ev         $Encodable
    */
  def insert[T](path: String, value: T, xattr: Boolean = false, createPath: Boolean = false, expandMacro: Boolean = false)
               (implicit ev: Encodable[T]): MutateInSpec = {
    val encoded = ev.encodeSubDocumentField(value)
    Insert(path, encoded, xattr, createPath, expandMacro)
  }

  /** Returns a `MutateInSpec` with the intent of replacing an existing value in a JSON object.
    *
    * Will error if the last element of the path does not exist.
    *
    * @param path       the path identifying where to replace the value.
    * @param value      the value to replace.  $SupportedTypes
    * @param xattr      $Xattr
    * @param expandMacro
    * @param ev         $Encodable
    */
  def replace[T](path: String, value: T, xattr: Boolean = false, expandMacro: Boolean = false)
                (implicit ev: Encodable[T]): MutateInSpec = {
    Replace(path, ev.encodeSubDocumentField(value), xattr, expandMacro)
  }

  /** Returns a `MutateInSpec` with the intent of upserting a value into a JSON object.
    *
    * That is, the value will be replaced if the path already exists, or inserted if not.
    *
    * @param path       the path identifying where to upsert the value.
    * @param value      the value to upsert.  $SupportedTypes
    * @param xattr      $Xattr
    * @param createPath $CreatePath
    * @param expandMacro
    * @param ev         $Encodable
    */
  def upsert[T](path: String, value: T, xattr: Boolean = false, createPath: Boolean = false, expandMacro: Boolean = false)
               (implicit ev: Encodable[T]): MutateInSpec = {
    Upsert(path, ev.encodeSubDocumentField(value), xattr, createPath, expandMacro)
  }

  /** Returns a `MutateInSpec` with the intent of removing a value from a JSON object.
    *
    * Will error if the last element of the path does not exist.
    *
    * @param path  the path identifying what to remove.
    * @param xattr $Xattr
    */
  def remove(path: String, xattr: Boolean = false): MutateInSpec = {
    Remove(path, xattr)
  }

  /** Returns a `MutateInSpec` with the intent of appending a value to an existing JSON array.
    *
    * Will error if the last element of the path does not exist or is not an array.
    *
    * @param path       the path identifying an array to which to append the value.
    * @param value      the value to append.  $SupportedTypes
    * @param xattr      $Xattr
    * @param createPath $CreatePath
    * @param expandMacro
    * @param ev         $Encodable
    */
  def arrayAppend[T](path: String, value: T, xattr: Boolean = false, createPath: Boolean = false, expandMacro: Boolean = false)
                    (implicit ev: Encodable[T]): MutateInSpec = {
    ArrayAppend(path, ev.encodeSubDocumentField(value), xattr, createPath, expandMacro)
  }

  /** Returns a `MutateInSpec` with the intent of prepending a value to an existing JSON array.
    *
    * Will error if the last element of the path does not exist or is not an array.
    *
    * @param path       the path identifying an array to which to prepend the value.
    * @param value      the value to append.  $SupportedTypes
    * @param xattr      $Xattr
    * @param createPath $CreatePath
    * @param expandMacro
    * @param ev         $Encodable
    */
  def arrayPrepend[T](path: String, value: T, xattr: Boolean = false, createPath: Boolean = false, expandMacro: Boolean = false)
                     (implicit ev: Encodable[T]): MutateInSpec = {
    ArrayPrepend(path, ev.encodeSubDocumentField(value), xattr, createPath, expandMacro)
  }

  /** Returns a `MutateInSpec` with the intent of inserting a value into an existing JSON array.
    *
    * Will error if the last element of the path does not exist or is not an array.
    *
    * @param path       the path identifying an array to which to append the value, and an index.  E.g. "foo.bar[3]"
    * @param value      the value to insert.  $SupportedTypes
    * @param xattr      $Xattr
    * @param createPath $CreatePath
    * @param expandMacro
    * @param ev         $Encodable
    */
  def arrayInsert[T](path: String, value: T, xattr: Boolean = false, createPath: Boolean = false, expandMacro: Boolean = false)
                    (implicit ev: Encodable[T]): MutateInSpec = {
    ArrayInsert(path, ev.encodeSubDocumentField(value), xattr, createPath, expandMacro)
  }

  // TODO add variants of these that take multiple values

  /** Returns a `MutateInSpec` with the intent of inserting a value into an existing JSON array, but only if the value
    * is not already contained in the array (by way of string comparison).
    *
    * Will error if the last element of the path does not exist or is not an array.
    *
    * @param path       the path identifying an array to which to append the value, and an index.  E.g. "foo.bar[3]"
    * @param value      the value to insert.  $SupportedTypes
    * @param xattr      $Xattr
    * @param createPath $CreatePath
    * @param expandMacro
    * @param ev         $Encodable
    */
  def arrayAddUnique[T](path: String, value: T, xattr: Boolean = false, createPath: Boolean = false, expandMacro: Boolean = false)
                       (implicit ev: Encodable[T]): MutateInSpec = {
    ArrayAddUnique(path, ev.encodeSubDocumentField(value), xattr, createPath, expandMacro)
  }

  /** Returns a `MutateInSpec` with the intent of incrementing a numerical field in a JSON object.
    *
    * If the field does not exist then it is created and takes the value of `delta`.
    *
    * @param path       the path identifying a numerical field to adjust or create.
    * @param delta      the value to increment the field by.
    * @param xattr      $Xattr
    * @param createPath $CreatePath
    * @param ev         $Encodable
    */
  def increment(path: String, delta: Long, xattr: Boolean = false, createPath: Boolean = false): MutateInSpec = {
    Increment(path, delta, xattr, createPath)
  }

  /** Returns a `MutateInSpec` with the intent of decrementing a numerical field in a JSON object.
    *
    * If the field does not exist then it is created and takes the value of `delta` * -1.
    * TODO verify this happens
    *
    * @param path       the path identifying a numerical field to adjust or create.
    * @param delta      the value to decrement the field by.
    * @param xattr      $Xattr
    * @param createPath $CreatePath
    * @param ev         $Encodable
    */
  def decrement(path: String, delta: Long, xattr: Boolean = false, createPath: Boolean = false): MutateInSpec = {
    Increment(path, delta * -1, xattr, createPath)
  }
}