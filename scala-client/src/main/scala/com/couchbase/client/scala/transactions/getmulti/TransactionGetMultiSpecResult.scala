/*
 * Copyright 2025 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.scala.transactions.getmulti

import com.couchbase.client.core.error.CouchbaseException
import com.couchbase.client.scala.transactions.TransactionGetResult

private[transactions] case class TransactionGetMultiSpecResult private (
    spec: TransactionGetMultiSpec,
    internal: Option[TransactionGetResult]
) {
  def get: TransactionGetResult = internal.getOrElse {
    throw new CouchbaseException(s"Called get() but document ${spec.id} is not present!")
  }

  def exists: Boolean = internal.isDefined
}
