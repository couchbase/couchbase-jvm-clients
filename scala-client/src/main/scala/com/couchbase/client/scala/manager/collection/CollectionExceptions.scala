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
package com.couchbase.client.scala.manager.collection

import com.couchbase.client.core.error.CouchbaseException
import com.couchbase.client.core.logging.RedactableArgument.redactMeta

case class ScopeNotFoundException(scopeName: String)
  extends CouchbaseException(s"Scope [${redactMeta(scopeName)}] not found.")

case class CollectionNotFoundException(collectionName: String)
  extends CouchbaseException(s"Collection [${redactMeta(collectionName)}] not found.")

case class ScopeAlreadyExistsException(scopeName: String)
  extends CouchbaseException(s"Scope [${redactMeta(scopeName)}] already exists.")

case class CollectionAlreadyExistsException(collectionName: String)
  extends CouchbaseException(s"Collection [${redactMeta(collectionName)}] already exists.")
