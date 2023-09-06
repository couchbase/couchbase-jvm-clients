/*
 * Copyright (c) 2019 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.scala.kv

import com.couchbase.client.core.api.kv.CoreSubdocGetCommand
import com.couchbase.client.core.msg.kv.SubdocCommandType

/** Methods to allow creating a sequence of `LookupInSpec` for providing to a `lookupIn` SubDocument method.
  *
  * @define Path           a valid path in the document, such as "foo.bar"
  * @define Xattr          Sets that this is an extended attribute (xattr) field (default is false).  Extended
  *                        Attributes (xattrs) are an advanced feature in which additional fields can be stored
  *                        alongside a document.
  *                        for a more detailed description.
  * @author Graham Pople
  * @since 1.0.0
  */
object LookupInSpec {

  /** Gets a field from a JSON document.
    *
    * To fetch the full document, use an empty path of "".
    *
    * @param path  $Path
    */
  def get(path: String): Get = {
    Get(path)
  }

  /** Gets the count of a path in a JSON document.  This only applies to JSON object and array fields.
    *
    * @param path  $Path
    */
  def count(path: String): Count = {
    Count(path)
  }

  /** Checks if a path exists in a JSON document.
    *
    * @param path  $Path
    */
  def exists(path: String): Exists = {
    Exists(path)
  }

  private[scala] def map(spec: collection.Seq[LookupInSpec]): Seq[CoreSubdocGetCommand] = {
    spec.map {
      case Get("", _xattr)      => new CoreSubdocGetCommand(SubdocCommandType.GET_DOC, "", _xattr)
      case Get(path, _xattr)    => new CoreSubdocGetCommand(SubdocCommandType.GET, path, _xattr)
      case Exists(path, _xattr) => new CoreSubdocGetCommand(SubdocCommandType.EXISTS, path, _xattr)
      case Count(path, _xattr)  => new CoreSubdocGetCommand(SubdocCommandType.COUNT, path, _xattr)
    }.toSeq // toSeq for 2.13
  }

}

/** Represents a single SubDocument lookup operation, such as fetching a particular field. */
sealed trait LookupInSpec

case class Get(private[scala] val path: String, private[scala] val _xattr: Boolean = false)
    extends LookupInSpec {

  /** Sets that this is an extended attribute (xattr) field (default is false).  Extended
    * Attributes (xattrs) are an advanced feature in which additional fields can be stored
    * alongside a document.
    *
    * @return an immutable copy of this, for chaining
    */
  def xattr: Get = {
    copy(path, _xattr = true)
  }
}

case class Exists(private[scala] val path: String, private[scala] val _xattr: Boolean = false)
    extends LookupInSpec {

  /** Sets that this is an extended attribute (xattr) field (default is false).  Extended
    * Attributes (xattrs) are an advanced feature in which additional fields can be stored
    * alongside a document.
    *
    * @return an immutable copy of this, for chaining
    */
  def xattr: Exists = {
    copy(path, _xattr = true)
  }
}

case class Count(private[scala] val path: String, private[scala] val _xattr: Boolean = false)
    extends LookupInSpec {

  /** Sets that this is an extended attribute (xattr) field (default is false).  Extended
    * Attributes (xattrs) are an advanced feature in which additional fields can be stored
    * alongside a document.
    *
    * @return an immutable copy of this, for chaining
    */
  def xattr: Count = {
    copy(path, _xattr = true)
  }
}
