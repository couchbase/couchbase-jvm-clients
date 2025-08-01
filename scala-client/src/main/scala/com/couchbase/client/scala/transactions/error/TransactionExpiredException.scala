/*
 * Copyright 2023 Couchbase, Inc.
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
package com.couchbase.client.scala.transactions.error;

import com.couchbase.client.core.error.transaction.internal.CoreTransactionExpiredException;

/** The transaction could not be fully completed in the configured timeout.
  *
  * It is in an undefined state, but it unambiguously did not reach the commit point.  No actors will be able to see the
  * contents of this transaction.
  */
class TransactionExpiredException private[scala] (
    private val internal: CoreTransactionExpiredException
) extends TransactionFailedException(internal)
