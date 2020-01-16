package com.couchbase.client.scala.kv

import com.couchbase.client.core.deps.io.netty.util.CharsetUtil
import com.couchbase.client.core.msg.kv.{SubdocCommandType, SubdocMutateRequest}
import com.couchbase.client.scala.codec.JsonSerializer
import com.couchbase.client.scala.util.RowTraversalUtil

import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Try}

/** Methods to allow constructing a sequence of `MutateInSpec`s.
  *
  * @define CreatePath     Sets that intermediate paths should be created (default is false)
  * @define Xattr          Sets that this is an extended attribute (xattr) field (default is false).  Extended
  *                        Attributes (xattrs) are an advanced feature in which additional fields can be stored
  *                        alongside a document.
  * @define SupportedTypes this can be of any type for which an implicit JsonSerializer can be found: a list
  *                        of types that are supported 'out of the box' is available at
  *                        [[https://docs.couchbase.com/scala-sdk/1.0/howtos/json.html these JSON docs]]
  * @define JsonSerializer      an implicit JsonSerializer.  For any supported type T this will be found automatically.
  * @author Graham Pople
  * @since 1.0.0
  */
object MutateInSpec {

  /** Returns a `MutateInSpec` with the intent of inserting a value into a JSON object.
    *
    * Will error if the last element of the path already exists.
    *
    * @param path       the path identifying where to insert the value.
    * @param value      the value to insert.  $SupportedTypes
    * @param serializer         $JsonSerializer
    */
  def insert[T](path: String, value: T)(implicit serializer: JsonSerializer[T]): Insert = {
    val expandMacro = value match {
      case v: MutateInMacro => true
      case _                => false
    }
    if (path == "")
      Insert(path, Failure(new IllegalArgumentException("Cannot pass an empty path to Insert")))
    else Insert(path, serializer.serialize(value), _expandMacro = expandMacro)
  }

  /** Returns a `MutateInSpec` with the intent of replacing an existing value in a JSON object.
    *
    * If the path is an empty string (""), the value replace the entire contents of the document.
    *
    * Will error if the last element of the path does not exist.
    *
    * @param path  the path identifying where to replace the value.
    * @param value the value to replace.  $SupportedTypes
    * @param serializer    $JsonSerializer
    */
  def replace[T](path: String, value: T)(implicit serializer: JsonSerializer[T]): Replace = {
    val expandMacro = value match {
      case v: MutateInMacro => true
      case _                => false
    }
    Replace(path, serializer.serialize(value), _expandMacro = expandMacro)
  }

  /** Returns a `MutateInSpec` with the intent of upserting a value into a JSON object.
    *
    * That is, the value will be replaced if the path already exists, or inserted if not.
    *
    * @param path       the path identifying where to upsert the value.
    * @param value      the value to upsert.  $SupportedTypes
    * @param serializer         $JsonSerializer
    */
  def upsert[T](path: String, value: T)(implicit serializer: JsonSerializer[T]): Upsert = {
    val expandMacro = value match {
      case v: MutateInMacro => true
      case _                => false
    }
    if (path == "")
      Upsert(path, Failure(new IllegalArgumentException("Cannot pass an empty path to Upsert")))
    else Upsert(path, serializer.serialize(value), _expandMacro = expandMacro)
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
    * @param values     the values to append.  $SupportedTypes
    * @param serializer         $JsonSerializer
    */
  def arrayAppend[T](path: String, values: collection.Seq[T])(
      implicit serializer: JsonSerializer[T]
  ): ArrayAppend = {
    if (values.size == 1) {
      val value = values.head
      val expandMacro = value match {
        case v: MutateInMacro => true
        case _                => false
      }
      ArrayAppend(path, serializer.serialize(value), _expandMacro = expandMacro)
    } else {
      ArrayAppend(path, encodeMulti(serializer, values))
    }
  }

  /** Encode all provided values into one, comma-separated, Array[Byte] */
  private def encodeMulti[T](
      serializer: JsonSerializer[T],
      values: Iterable[T]
  ): Try[Array[Byte]] = {
    if (values.isEmpty) {
      Failure(new IllegalArgumentException("Empty set of values provided"))
    } else {
      val encoded = values.map(value => {
        serializer.serialize(value)
      })

      // Turn Seq[Try] to Try[Seq]
      val traversed = RowTraversalUtil.traverse(encoded.iterator)

      traversed.map(v => {
        val out = new ArrayBuffer[Byte]()

        v.foreach { bytes =>
          out ++= bytes += ',' // multiple values are comma separated
        }

        // Should be covered by values.isEmpty check, but to be safe
        if (out.nonEmpty) {
          out.remove(out.size - 1) // remove the trailing ','
        }

        out.toArray
      })
    }
  }

  /** Returns a `MutateInSpec` with the intent of prepending a value to an existing JSON array.
    *
    * Will error if the last element of the path does not exist or is not an array.
    *
    * @param path       the path identifying an array to which to prepend the value.
    * @param values     the value(s) to prepend.  $SupportedTypes
    * @param serializer         $JsonSerializer
    */
  def arrayPrepend[T](path: String, values: collection.Seq[T])(
      implicit serializer: JsonSerializer[T]
  ): ArrayPrepend = {
    values match {
      case value :: Nil =>
        ArrayPrepend(
          path,
          serializer.serialize(value),
          _expandMacro = value.isInstanceOf[MutateInMacro]
        )
      case _ =>
        ArrayPrepend(path, encodeMulti(serializer, values))
    }
  }

  /** Returns a `MutateInSpec` with the intent of inserting a value into an existing JSON array.
    *
    * Will error if the last element of the path does not exist or is not an array.
    *
    * @param path       the path identifying an array to which to append the value, and an index.  E.g. "foo.bar[3]"
    * @param values      the value(s) to insert.  $SupportedTypes
    * @param serializer         $JsonSerializer
    */
  def arrayInsert[T](path: String, values: collection.Seq[T])(
      implicit serializer: JsonSerializer[T]
  ): ArrayInsert = {
    values match {
      case value :: Nil =>
        ArrayInsert(
          path,
          serializer.serialize(value),
          _expandMacro = value.isInstanceOf[MutateInMacro]
        )
      case _ =>
        ArrayInsert(path, encodeMulti(serializer, values))
    }
  }

  /** Returns a `MutateInSpec` with the intent of inserting a value into an existing JSON array, but only if the value
    * is not already contained in the array (by way of string comparison).
    *
    * Will error if the last element of the path does not exist or is not an array.
    *
    * @param path       the path identifying an array to which to append the value, and an index.  E.g. "foo.bar[3]"
    * @param value      the value to insert.  $SupportedTypes
    */
  def arrayAddUnique[T](path: String, value: T)(
      implicit serializer: JsonSerializer[T]
  ): ArrayAddUnique = {
    ArrayAddUnique(
      path,
      serializer.serialize(value),
      _expandMacro = value.isInstanceOf[MutateInMacro]
    )
  }

  /** Returns a `MutateInSpec` with the intent of incrementing a numerical field in a JSON object.
    *
    * If the field does not exist then it is created and takes the value of `delta`.
    *
    * @param path       the path identifying a numerical field to adjust or create.
    * @param delta      the value to increment the field by.
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
    */
  def decrement(path: String, delta: Long): Increment = {
    Increment(path, delta * -1)
  }
}

/** Represents an intent to perform a single SubDocument mutation. */
sealed trait MutateInSpec {
  private[scala] def convert(originalIndex: Int): SubdocMutateRequest.Command

  private[scala] val typ: SubdocCommandType
}

/** Most SubDocument mutations are pretty similar, encapsulate the similarities here. */
trait MutateInSpecStandard extends MutateInSpec {
  private[scala] val path: String
  private[scala] val fragment: Try[Array[Byte]]
  private[scala] val _xattr: Boolean
  private[scala] val _createPath: Boolean
  private[scala] val _expandMacro: Boolean

  private[scala] def convert(originalIndex: Int) =
    new SubdocMutateRequest.Command(
      typ,
      path,
      value,
      _createPath,
      _xattr,
      _expandMacro,
      originalIndex
    )

  private[scala] def value = fragment.get
}

case class Insert(
    path: String,
    fragment: Try[Array[Byte]],
    private[scala] override val _xattr: Boolean = false,
    private[scala] override val _createPath: Boolean = false,
    private[scala] override val _expandMacro: Boolean = false
) extends MutateInSpecStandard {
  override val typ: SubdocCommandType = SubdocCommandType.DICT_ADD

  /** Sets that this is an extended attribute (xattr) field (default is false).  Extended
    * Attributes (xattrs) are an advanced feature in which additional fields can be stored
    * alongside a document.
    *
    * @return an immutable copy of this, for chaining
    */
  def xattr: Insert = {
    copy(path, fragment, _xattr = true, _createPath = _createPath, _expandMacro = _expandMacro)
  }

  /**  Sets that intermediate paths should be created (default is false)
    *
    * @return an immutable copy of this, for chaining
    */
  def createPath: Insert = {
    copy(path, fragment, _xattr, _createPath = true, _expandMacro)
  }
}

case class Replace(
    path: String,
    fragment: Try[Array[Byte]],
    private[scala] val _xattr: Boolean = false,
    private[scala] val _expandMacro: Boolean = false
) extends MutateInSpec {
  override val typ: SubdocCommandType = path match {
    case "" => SubdocCommandType.SET_DOC
    case _  => SubdocCommandType.REPLACE
  }

  /** Sets that this is an extended attribute (xattr) field (default is false).  Extended
    * Attributes (xattrs) are an advanced feature in which additional fields can be stored
    * alongside a document.
    *
    * @return an immutable copy of this, for chaining
    */
  def xattr: Replace = {
    copy(path, fragment, _xattr = true, _expandMacro = _expandMacro)
  }

  def convert(originalIndex: Int) =
    new SubdocMutateRequest.Command(
      typ,
      path,
      fragment.get,
      false,
      _xattr,
      _expandMacro,
      originalIndex
    )
}

case class Upsert(
    path: String,
    fragment: Try[Array[Byte]],
    private[scala] override val _xattr: Boolean = false,
    private[scala] override val _createPath: Boolean = false,
    private[scala] override val _expandMacro: Boolean = false
) extends MutateInSpecStandard {
  override val typ: SubdocCommandType = SubdocCommandType.DICT_UPSERT

  /** Sets that this is an extended attribute (xattr) field (default is false).  Extended
    * Attributes (xattrs) are an advanced feature in which additional fields can be stored
    * alongside a document.
    *
    * @return an immutable copy of this, for chaining
    */
  def xattr: Upsert = {
    copy(path, fragment, _xattr = true, _createPath = _createPath, _expandMacro = _expandMacro)
  }

  /**  Sets that intermediate paths should be created (default is false)
    *
    * @return an immutable copy of this, for chaining
    */
  def createPath: Upsert = {
    copy(path, fragment, _xattr, _createPath = true, _expandMacro)
  }
}

case class Remove(path: String, private[scala] val _xattr: Boolean = false) extends MutateInSpec {
  override val typ: SubdocCommandType = SubdocCommandType.DELETE

  /** Sets that this is an extended attribute (xattr) field (default is false).  Extended
    * Attributes (xattrs) are an advanced feature in which additional fields can be stored
    * alongside a document.
    *
    * @return an immutable copy of this, for chaining
    */
  def xattr: Remove = {
    copy(path, _xattr = true)
  }

  def convert(originalIndex: Int) =
    new SubdocMutateRequest.Command(typ, path, Array[Byte](), false, _xattr, false, originalIndex)
}

case class ArrayAppend(
    path: String,
    fragment: Try[Array[Byte]],
    private[scala] override val _xattr: Boolean = false,
    private[scala] override val _createPath: Boolean = false,
    private[scala] override val _expandMacro: Boolean = false
) extends MutateInSpecStandard {
  override val typ: SubdocCommandType = SubdocCommandType.ARRAY_PUSH_LAST

  /** Sets that this is an extended attribute (xattr) field (default is false).  Extended
    * Attributes (xattrs) are an advanced feature in which additional fields can be stored
    * alongside a document.
    *
    * @return an immutable copy of this, for chaining
    */
  def xattr: ArrayAppend = {
    copy(path, fragment, _xattr = true, _createPath = _createPath, _expandMacro = _expandMacro)
  }

  /**  Sets that intermediate paths should be created (default is false)
    *
    * @return an immutable copy of this, for chaining
    */
  def createPath: ArrayAppend = {
    copy(path, fragment, _xattr, _createPath = true, _expandMacro)
  }
}

case class ArrayPrepend(
    path: String,
    fragment: Try[Array[Byte]],
    private[scala] override val _xattr: Boolean = false,
    private[scala] override val _createPath: Boolean = false,
    private[scala] override val _expandMacro: Boolean = false
) extends MutateInSpecStandard {
  override val typ: SubdocCommandType = SubdocCommandType.ARRAY_PUSH_FIRST

  /** Sets that this is an extended attribute (xattr) field (default is false).  Extended
    * Attributes (xattrs) are an advanced feature in which additional fields can be stored
    * alongside a document.
    *
    * @return an immutable copy of this, for chaining
    */
  def xattr: ArrayPrepend = {
    copy(path, fragment, _xattr = true, _createPath = _createPath, _expandMacro = _expandMacro)
  }

  /**  Sets that intermediate paths should be created (default is false)
    *
    * @return an immutable copy of this, for chaining
    */
  def createPath: ArrayPrepend = {
    copy(path, fragment, _xattr, _createPath = true, _expandMacro)
  }
}

case class ArrayInsert(
    path: String,
    fragment: Try[Array[Byte]],
    private[scala] override val _xattr: Boolean = false,
    private[scala] override val _createPath: Boolean = false,
    private[scala] override val _expandMacro: Boolean = false
) extends MutateInSpecStandard {
  override val typ: SubdocCommandType = SubdocCommandType.ARRAY_INSERT

  /** Sets that this is an extended attribute (xattr) field (default is false).  Extended
    * Attributes (xattrs) are an advanced feature in which additional fields can be stored
    * alongside a document.
    *
    * @return an immutable copy of this, for chaining
    */
  def xattr: ArrayInsert = {
    copy(path, fragment, _xattr = true, _createPath = _createPath, _expandMacro = _expandMacro)
  }

  /**  Sets that intermediate paths should be created (default is false)
    *
    * @return an immutable copy of this, for chaining
    */
  def createPath: ArrayInsert = {
    copy(path, fragment, _xattr, _createPath = true, _expandMacro)
  }
}

case class ArrayAddUnique(
    path: String,
    fragment: Try[Array[Byte]],
    private[scala] override val _xattr: Boolean = false,
    private[scala] override val _createPath: Boolean = false,
    private[scala] override val _expandMacro: Boolean = false
) extends MutateInSpecStandard {
  override val typ: SubdocCommandType = SubdocCommandType.ARRAY_ADD_UNIQUE

  /** Sets that this is an extended attribute (xattr) field (default is false).  Extended
    * Attributes (xattrs) are an advanced feature in which additional fields can be stored
    * alongside a document.
    *
    * @return an immutable copy of this, for chaining
    */
  def xattr: ArrayAddUnique = {
    copy(path, fragment, _xattr = true, _createPath = _createPath, _expandMacro = _expandMacro)
  }

  /**  Sets that intermediate paths should be created (default is false)
    *
    * @return an immutable copy of this, for chaining
    */
  def createPath: ArrayAddUnique = {
    copy(path, fragment, _xattr, _createPath = true, _expandMacro)
  }
}

case class Increment(
    path: String,
    delta: Long,
    private[scala] val _xattr: Boolean = false,
    private[scala] val _createPath: Boolean = false
) extends MutateInSpec {
  override val typ: SubdocCommandType = SubdocCommandType.COUNTER

  /** Sets that this is an extended attribute (xattr) field (default is false).  Extended
    * Attributes (xattrs) are an advanced feature in which additional fields can be stored
    * alongside a document.
    *
    * @return an immutable copy of this, for chaining
    */
  def xattr: Increment = {
    copy(path, delta, _xattr = true, _createPath)
  }

  /**  Sets that intermediate paths should be created (default is false)
    *
    * @return an immutable copy of this, for chaining
    */
  def createPath: Increment = {
    copy(path, delta, _xattr, _createPath = true)
  }

  def convert(originalIndex: Int) = {
    val bytes = delta.toString.getBytes(CharsetUtil.UTF_8)
    new SubdocMutateRequest.Command(typ, path, bytes, _createPath, _xattr, false, originalIndex)
  }

}
