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

/** Methods to allow creating a sequence of `LookupInSpec` for providing to a `lookupIn` SubDocument method.
  *
  * @define Path  a valid path in the document, such as "foo.bar"
  * @define Xattr Extended Attributes (xattrs) are an advanced feature in which additional fields can be stored
  *               alongside a document.  See **CHANGEME** for a more detailed description.
  * @author Graham Pople
  * @since 1.0.0
  */
object LookupInSpec {
  /** Gets a field from a JSON document.
    *
    * @param path  $Path
    * @param xattr $Xattr
    */
  def get(path: String, xattr: Boolean = false): LookupInSpec = {
    Get(path, xattr)
  }

  /** Gets the count of a path in a JSON document.  This only applies to JSON object and array fields.
    *
    * @param path  $Path
    * @param xattr $Xattr
    */
  def count(path: String, xattr: Boolean = false): LookupInSpec = {
    Count(path, xattr)
  }

  /** Checks if a path exists in a JSON document.
    *
    * @param path  $Path
    * @param xattr $Xattr
    */
  def exists(path: String, xattr: Boolean = false): LookupInSpec = {
    Exists(path, xattr)
  }

  /** Requests that the full document should be fetched.
    *
    * This is provided to support some advanced workloads that need to fetch the document along with some extended
    * attributes (xattrs).
    */
  def getDoc: LookupInSpec = {
    GetFullDocument()
  }
}

/** Represents a single SubDocument lookup operation, such as fetching a particular field. */
sealed trait LookupInSpec

private case class Get(path: String, xattr: Boolean) extends LookupInSpec

private case class GetFullDocument() extends LookupInSpec

private case class Exists(path: String, xattr: Boolean) extends LookupInSpec

private case class Count(path: String, xattr: Boolean) extends LookupInSpec
