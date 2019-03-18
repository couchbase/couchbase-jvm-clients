package com.couchbase.client.scala.kv

import com.couchbase.client.core.msg.kv.{SubdocCommandType, SubdocMutateRequest}
import com.couchbase.client.scala.codec.Conversions.Encodable
import com.couchbase.client.scala.codec.{Conversions, EncodeParams}
import com.couchbase.client.core.deps.io.netty.util.CharsetUtil

import scala.util.Try

/** Methods to allow constructing a sequence of `MutateInSpec`s.
  *
  * @define CreatePath     Sets that intermediate paths should be created (default is false)
  * @define Xattr          Sets that this is an extended attribute (xattr) field (default is false).  Extended
  *                        Attributes (xattrs) are an advanced feature in which additional fields can be stored
  *                        alongside a document.  See **CHANGEME** for a more detailed description.
  * @define SupportedTypes this can be of any type for which an implicit Encodable can be found: a list
  *                        of types that are supported 'out of the box' is available at ***CHANGEME:TYPES***
  * @define Encodable      an implicit Encodable.  For any supported type T this will be found automatically.
  * @author Graham Pople
  * @since 1.0.0
  */
object MutateInSpec {
  // TODO fix ***CHANGEME*** when docs are available

  /** Returns a `MutateInSpec` with the intent of inserting a value into a JSON object.
    *
    * Will error if the last element of the path already exists.
    *
    * @param path       the path identifying where to insert the value.
    * @param value      the value to insert.  $SupportedTypes
    * @param ev         $Encodable
    */
  def insert[T](path: String, value: T)
               (implicit ev: Encodable[T]): Insert = {
    val expandMacro = value match {
      case v: MutateInMacro => true
      case _ => false
    }
    Insert(path, ev.encodeSubDocumentField(value), _expandMacro = expandMacro)
  }

  /** Returns a `MutateInSpec` with the intent of replacing an existing value in a JSON object.
    *
    * Will error if the last element of the path does not exist.
    *
    * @param path  the path identifying where to replace the value.
    * @param value the value to replace.  $SupportedTypes
    * @param ev    $Encodable
    */
  def replace[T](path: String, value: T)
                (implicit ev: Encodable[T]): Replace = {
    val expandMacro = value match {
      case v: MutateInMacro => true
      case _ => false
    }
    Replace(path, ev.encodeSubDocumentField(value), _expandMacro = expandMacro)
  }

  /** Returns a `MutateInSpec` with the intent of upserting a value into a JSON object.
    *
    * That is, the value will be replaced if the path already exists, or inserted if not.
    *
    * @param path       the path identifying where to upsert the value.
    * @param value      the value to upsert.  $SupportedTypes
    * @param xattr      $Xattr
    * @param ev         $Encodable
    */
  def upsert[T](path: String, value: T)
               (implicit ev: Encodable[T]): Upsert = {
    val expandMacro = value match {
      case v: MutateInMacro => true
      case _ => false
    }
    Upsert(path, ev.encodeSubDocumentField(value), _expandMacro = expandMacro)
  }

  /** Returns a `MutateInSpec` with the intent of removing a value from a JSON object.
    *
    * Will error if the last element of the path does not exist.
    *
    * @param path  the path to be removed.
    */
  def remove(path: String): Remove = {
    Remove(path)
  }

  /** Returns a `MutateInSpec` with the intent of appending a value to an existing JSON array.
    *
    * Will error if the last element of the path does not exist or is not an array.
    *
    * @param path       the path identifying an array to which to append the value.
    * @param value      the value to append.  $SupportedTypes
    * @param ev         $Encodable
    */
  def arrayAppend[T](path: String, value: T)
                    (implicit ev: Encodable[T]): ArrayAppend = {
    val expandMacro = value match {
      case v: MutateInMacro => true
      case _ => false
    }
    ArrayAppend(path, ev.encodeSubDocumentField(value), _expandMacro = expandMacro)
  }

  /** Returns a `MutateInSpec` with the intent of prepending a value to an existing JSON array.
    *
    * Will error if the last element of the path does not exist or is not an array.
    *
    * @param path       the path identifying an array to which to prepend the value.
    * @param value      the value to append.  $SupportedTypes
    * @param ev         $Encodable
    */
  def arrayPrepend[T](path: String, value: T)
                     (implicit ev: Encodable[T]): ArrayPrepend = {
    val expandMacro = value match {
      case v: MutateInMacro => true
      case _ => false
    }
    ArrayPrepend(path, ev.encodeSubDocumentField(value), _expandMacro = expandMacro)
  }

  /** Returns a `MutateInSpec` with the intent of inserting a value into an existing JSON array.
    *
    * Will error if the last element of the path does not exist or is not an array.
    *
    * @param path       the path identifying an array to which to append the value, and an index.  E.g. "foo.bar[3]"
    * @param value      the value to insert.  $SupportedTypes
    * @param xattr      $Xattr
    * @param createPath $CreatePath
    * @param ev         $Encodable
    */
  def arrayInsert[T](path: String, value: T)
                    (implicit ev: Encodable[T]): ArrayInsert = {
    val expandMacro = value match {
      case v: MutateInMacro => true
      case _ => false
    }
    ArrayInsert(path, ev.encodeSubDocumentField(value), _expandMacro = expandMacro)
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
    * @param ev         $Encodable
    */
  def arrayAddUnique[T](path: String, value: T)
                       (implicit ev: Encodable[T]): ArrayAddUnique = {
    val expandMacro = value match {
      case v: MutateInMacro => true
      case _ => false
    }
    ArrayAddUnique(path, ev.encodeSubDocumentField(value), _expandMacro = expandMacro)
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
  def increment(path: String, delta: Long): Increment = {
    Increment(path, delta)
  }

  /** Returns a `MutateInSpec` with the intent of decrementing a numerical field in a JSON object.
    *
    * If the field does not exist then it is created and takes the value of `delta` * -1.
    *
    * @param path       the path identifying a numerical field to adjust or create.
    * @param delta      the value to decrement the field by.
    * @param xattr      $Xattr
    * @param createPath $CreatePath
    * @param ev         $Encodable
    */
  def decrement(path: String, delta: Long): Increment = {
    Increment(path, delta * -1)
  }
}


/** Represents an intent to perform a single SubDocument mutation. */
sealed trait MutateInSpec {
  private[scala] def convert: SubdocMutateRequest.Command

  private[scala] val typ: SubdocCommandType
}

/** Most SubDocument mutations are pretty similar, encapsulate the similarities here. */
trait MutateInSpecStandard extends MutateInSpec {
  private[scala] val path: String
  private[scala] val fragment: Try[(Array[Byte], EncodeParams)]
  private[scala] val _xattr: Boolean
  private[scala] val _createPath: Boolean
  private[scala] val _expandMacro: Boolean

  private[scala] def convert = new SubdocMutateRequest.Command(typ, path, value, _createPath, _xattr, _expandMacro)

  private[scala] def value = fragment.get._1
}

case class Insert(path: String,
                  fragment: Try[(Array[Byte], EncodeParams)],
                  private[scala] override val _xattr: Boolean = false,
                  private[scala] override val _createPath: Boolean = false,
                  private[scala] override val _expandMacro: Boolean = false
                 ) extends MutateInSpecStandard {
  override val typ: SubdocCommandType = SubdocCommandType.DICT_ADD

  /** $Xattr */
  def xattr: Insert = {
    copy(path, fragment, _xattr = true, _createPath = _createPath, _expandMacro = _expandMacro)
  }

  /** $CreatePath */
  def createPath: Insert = {
    copy(path, fragment, _xattr, _createPath = true, _expandMacro)
  }
}

case class Replace(path: String,
                   fragment: Try[(Array[Byte], EncodeParams)],
                   private[scala] val _xattr: Boolean = false,
                   private[scala] val _expandMacro: Boolean = false
                  ) extends MutateInSpec {
  override val typ: SubdocCommandType = SubdocCommandType.REPLACE

  /** $Xattr */
  def xattr: Replace = {
    copy(path, fragment, _xattr = true, _expandMacro = _expandMacro)
  }

  def convert = new SubdocMutateRequest.Command(typ, path, fragment.get._1, false, _xattr, _expandMacro)
}

case class Upsert(path: String,
                  fragment: Try[(Array[Byte], EncodeParams)],
                  private[scala] override val _xattr: Boolean = false,
                  private[scala] override val _createPath: Boolean = false,
                  private[scala] override val _expandMacro: Boolean = false
                 ) extends MutateInSpecStandard {
  override val typ: SubdocCommandType = SubdocCommandType.DICT_UPSERT

  /** $Xattr */
  def xattr: Upsert = {
    copy(path, fragment, _xattr = true, _createPath = _createPath, _expandMacro = _expandMacro)
  }

  /** $CreatePath */
  def createPath: Upsert = {
    copy(path, fragment, _xattr, _createPath = true, _expandMacro)
  }
}

case class Remove(path: String,
                  private[scala] val _xattr: Boolean = false) extends MutateInSpec {
  override val typ: SubdocCommandType = SubdocCommandType.DELETE

  /** $Xattr */
  def xattr: Remove = {
    copy(path, _xattr = true)
  }

  def convert = new SubdocMutateRequest.Command(typ, path, Array[Byte](), false, _xattr, false)
}

case class ArrayAppend(path: String,
                       fragment: Try[(Array[Byte], EncodeParams)],
                       private[scala] override val _xattr: Boolean = false,
                       private[scala] override val _createPath: Boolean = false,
                       private[scala] override val _expandMacro: Boolean = false
                      ) extends MutateInSpecStandard {
  override val typ: SubdocCommandType = SubdocCommandType.ARRAY_PUSH_LAST

  /** $Xattr */
  def xattr: ArrayAppend = {
    copy(path, fragment, _xattr = true, _createPath = _createPath, _expandMacro = _expandMacro)
  }

  /** $CreatePath */
  def createPath: ArrayAppend = {
    copy(path, fragment, _xattr, _createPath = true, _expandMacro)
  }
}

case class ArrayPrepend(path: String,
                        fragment: Try[(Array[Byte], EncodeParams)],
                        private[scala] override val _xattr: Boolean = false,
                        private[scala] override val _createPath: Boolean = false,
                        private[scala] override val _expandMacro: Boolean = false
                       ) extends MutateInSpecStandard {
  override val typ: SubdocCommandType = SubdocCommandType.ARRAY_PUSH_FIRST

  /** $Xattr */
  def xattr: ArrayPrepend = {
    copy(path, fragment, _xattr = true, _createPath = _createPath, _expandMacro = _expandMacro)
  }

  /** $CreatePath */
  def createPath: ArrayPrepend = {
    copy(path, fragment, _xattr, _createPath = true, _expandMacro)
  }
}

case class ArrayInsert(path: String,
                       fragment: Try[(Array[Byte], EncodeParams)],
                       private[scala] override val _xattr: Boolean = false,
                       private[scala] override val _createPath: Boolean = false,
                       private[scala] override val _expandMacro: Boolean = false
                      ) extends MutateInSpecStandard {
  override val typ: SubdocCommandType = SubdocCommandType.ARRAY_INSERT

  /** $Xattr */
  def xattr: ArrayInsert = {
    copy(path, fragment, _xattr = true, _createPath = _createPath, _expandMacro = _expandMacro)
  }

  /** $CreatePath */
  def createPath: ArrayInsert = {
    copy(path, fragment, _xattr, _createPath = true, _expandMacro)
  }
}

case class ArrayAddUnique(path: String,
                          fragment: Try[(Array[Byte], EncodeParams)],
                          private[scala] override val _xattr: Boolean = false,
                          private[scala] override val _createPath: Boolean = false,
                          private[scala] override val _expandMacro: Boolean = false
                         ) extends MutateInSpecStandard {
  override val typ: SubdocCommandType = SubdocCommandType.ARRAY_ADD_UNIQUE

  /** $Xattr */
  def xattr: ArrayAddUnique = {
    copy(path, fragment, _xattr = true, _createPath = _createPath, _expandMacro = _expandMacro)
  }

  /** $CreatePath */
  def createPath: ArrayAddUnique = {
    copy(path, fragment, _xattr, _createPath = true, _expandMacro)
  }
}

case class Increment(path: String,
                     delta: Long,
                     private[scala] val _xattr: Boolean = false,
                     private[scala] val _createPath: Boolean = false
                    ) extends MutateInSpec {
  override val typ: SubdocCommandType = SubdocCommandType.COUNTER

  /** $Xattr */
  def xattr: Increment = {
    copy(path, delta, _xattr = true, _createPath)
  }

  /** $CreatePath */
  def createPath: Increment = {
    copy(path, delta, _xattr, _createPath = true)
  }

  def convert = {
    val bytes = delta.toString.getBytes(CharsetUtil.UTF_8)
    new SubdocMutateRequest.Command(typ, path, bytes, _createPath, _xattr, false)
  }
}

