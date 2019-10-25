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

/** Specifies whether a document should be created, and how, if it does not already exist. */
sealed class StoreSemantics

object StoreSemantics {

  /** If the document does not exist then do nothing, and fail the operation with `DocumentDoesNotExistException`. */
  case object Replace extends StoreSemantics

  /** If - and only if - the document does not exist, create it before applying the operation.  If it does exist, fail
    * the operation with `DocumentAlreadyExistsException`.
    */
  case object Insert extends StoreSemantics

  /** Create the document does not exist, or do nothing if it does.  Then apply the operation. */
  case object Upsert extends StoreSemantics
}
