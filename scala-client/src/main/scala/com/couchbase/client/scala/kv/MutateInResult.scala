package com.couchbase.client.scala.kv

import java.util.NoSuchElementException

import com.couchbase.client.core.msg.kv.{MutationToken, SubDocumentField}
import com.couchbase.client.scala.api.HasDurabilityTokens
import com.couchbase.client.scala.codec.{Conversions, JsonDeserializer}

import scala.util.{Failure, Try}
import scala.compat.java8.OptionConverters._

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
    private val content: Array[SubDocumentField],
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
    if (index < 0 || index >= content.length) {
      Failure(new IllegalArgumentException(s"$index is out of bounds"))
    } else {
      val field = content(index)
      if (field == null) {
        Failure(new NoSuchElementException(s"No result exists at index ${index}"))
      } else {
        field.error().asScala match {
          case Some(err) => Failure(err)
          case _         => deserializer.deserialize(field.value)
        }
      }
    }
  }
}
