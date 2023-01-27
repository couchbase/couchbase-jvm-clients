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

import com.couchbase.client.core.api.kv.CoreSubdocMutateResult
import com.couchbase.client.core.msg.kv.MutationToken
import com.couchbase.client.scala.codec.JsonDeserializer

import scala.compat.java8.OptionConverters._
import scala.util.{Failure, Try}

/** The results of a SubDocument `mutateIn` operation.
  *
  * When doing a `mutateIn` the application provides a sequence of [[MutateInSpec]].  The indexes into this sequence
  * are used when retrieving the results.
  *
  * @param id            the unique identifier of the document
  * @param cas           the document's CAS value at the time of the lookup
  * @param mutationToken if the [[com.couchbase.client.scala.env.ClusterEnvironment]]'s `ioConfig()
  *                      .mutationTokensEnabled()` field is true (which is recommended), this will contain a
  *                      `MutationToken` providing additional context on the mutation.
  *
  * @define Index          the index of the [[MutateInSpec]] provided to the `mutateIn`
  * @author Graham Pople
  * @since 1.0.0
  * */
case class MutateInResult(
    id: String,
    private val content: CoreSubdocMutateResult,
    cas: Long,
    mutationToken: Option[MutationToken]
) extends HasDurabilityTokens {

  /** Retrieve the content returned for a particular `MutateInSpec`, converted into the application's preferred
    * representation.
    *
    * This is only applicable for counter operations, which return the content of the counter post-mutation.
    *
    * @param index $Index
    * @tparam T as this is only applicable for counters, the application should pass T=Long
    */
  def contentAs[T](index: Int)(implicit deserializer: JsonDeserializer[T]): Try[T] = {
    Try(content.field(index))
      .flatMap(field => {
        field.error().asScala match {
          case Some(err) => Failure(err)
          case _ =>
            deserializer.deserialize(field.value())
        }
      })
  }
}
