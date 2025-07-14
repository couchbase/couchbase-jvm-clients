/*
 * Copyright (c) 2023 Couchbase, Inc.
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
// [skip:<1.5.0]
package com.couchbase.utils

import com.couchbase.client.core.cnc.events.transaction.{
  TransactionCleanupAttemptEvent,
  TransactionLogEvent
}
import com.couchbase.client.core.error._
import com.couchbase.client.core.error.transaction._
import com.couchbase.client.core.transaction.components.ActiveTransactionRecordEntry
import com.couchbase.client.core.transaction.support.AttemptState
import com.couchbase.client.performer.scala.error.InternalPerformerFailure
import com.couchbase.client.protocol.transactions._
import com.couchbase.client.scala.transactions.TransactionResult
import com.couchbase.client.scala.transactions.error.{
  TransactionCommitAmbiguousException,
  TransactionExpiredException,
  TransactionFailedException
}

import scala.collection.JavaConverters._

object ResultsUtil {
  def createResult(
      exception: Option[Exception],
      transactionResult: TransactionResult,
      cleanupRequests: Option[Integer]
  ): com.couchbase.client.protocol.transactions.TransactionResult = createResult(
    exception,
    Some(transactionResult.logs.toSeq),
    transactionResult.transactionId,
    transactionResult.unstagingComplete,
    cleanupRequests
  )

  def createResult(
      exception: Option[Exception],
      logs: Option[collection.Seq[TransactionLogEvent]],
      transactionId: String,
      unstagingComplete: Boolean,
      cleanupRequests: Option[Integer]
  ): com.couchbase.client.protocol.transactions.TransactionResult = {
    val response =
      com.couchbase.client.protocol.transactions.TransactionResult.getDefaultInstance.newBuilderForType
    exception match {
      case Some(ex) =>
        response.setException(convertTransactionFailed(ex))
        response.setExceptionCause(mapCause(ex.getCause))
      case None =>
        response.setException(TransactionException.NO_EXCEPTION_THROWN)
    }
    cleanupRequests match {
      case Some(_) =>
        response.setCleanupRequestsValid(true)
        response.setCleanupRequestsPending(cleanupRequests.get)
      case None =>
        response.setCleanupRequestsValid(false)
    }

    response.setTransactionId(transactionId)
    response.setUnstagingComplete(unstagingComplete)
    logs match {
      case Some(value) => value.foreach((l: TransactionLogEvent) => response.addLog(l.toString))
      case None        =>
    }
    response.build
  }

  def convertTransactionFailed(ex: Exception): TransactionException = ex match {
    case _: TransactionExpiredException         => TransactionException.EXCEPTION_EXPIRED
    case _: TransactionCommitAmbiguousException => TransactionException.EXCEPTION_COMMIT_AMBIGUOUS
    case _: TransactionFailedException          => TransactionException.EXCEPTION_FAILED
    case _                                      => TransactionException.EXCEPTION_UNKNOWN
  }

  def mapState(state: AttemptState): AttemptStates = state match {
    case AttemptState.ABORTED =>
      com.couchbase.client.protocol.transactions.AttemptStates.ABORTED
    case AttemptState.COMMITTED =>
      com.couchbase.client.protocol.transactions.AttemptStates.COMMITTED
    case AttemptState.NOT_STARTED =>
      com.couchbase.client.protocol.transactions.AttemptStates.NOTHING_WRITTEN
    case AttemptState.COMPLETED =>
      com.couchbase.client.protocol.transactions.AttemptStates.COMPLETED
    case AttemptState.PENDING =>
      com.couchbase.client.protocol.transactions.AttemptStates.PENDING
    case AttemptState.ROLLED_BACK =>
      com.couchbase.client.protocol.transactions.AttemptStates.ROLLED_BACK
    case _ =>
      com.couchbase.client.protocol.transactions.AttemptStates.UNKNOWN
  }
  def mapCause(ex: Throwable): ExternalException =
    if (ex == null) ExternalException.NotSet
    else
      ex match {
        case _: ActiveTransactionRecordEntryNotFoundException =>
          ExternalException.ActiveTransactionRecordEntryNotFound
        case _: ActiveTransactionRecordFullException =>
          ExternalException.ActiveTransactionRecordFull
        case _: ActiveTransactionRecordNotFoundException =>
          ExternalException.ActiveTransactionRecordNotFound
        case _: DocumentAlreadyInTransactionException =>
          ExternalException.DocumentAlreadyInTransaction
        case _: DocumentExistsException          => ExternalException.DocumentExistsException
        case _: DocumentUnretrievableException   => ExternalException.DocumentUnretrievableException
        case _: DocumentNotFoundException        => ExternalException.DocumentNotFoundException
        case _: FeatureNotAvailableException     => ExternalException.FeatureNotAvailableException
        case _: PreviousOperationFailedException => ExternalException.PreviousOperationFailed
        case _: ForwardCompatibilityFailureException =>
          ExternalException.ForwardCompatibilityFailure
        case _: ParsingFailureException      => ExternalException.ParsingFailure
        case _: IllegalStateException        => ExternalException.IllegalStateException
        case _: ServiceNotAvailableException => ExternalException.ServiceNotAvailableException
        case _: ConcurrentOperationsDetectedOnSameDocumentException =>
          ExternalException.ConcurrentOperationsDetectedOnSameDocument
        case _: CommitNotPermittedException          => ExternalException.CommitNotPermitted
        case _: RollbackNotPermittedException        => ExternalException.RollbackNotPermitted
        case _: TransactionAlreadyAbortedException   => ExternalException.TransactionAlreadyAborted
        case _: TransactionAlreadyCommittedException =>
          ExternalException.TransactionAlreadyCommitted
        case _: UnambiguousTimeoutException    => ExternalException.UnambiguousTimeoutException
        case _: AmbiguousTimeoutException      => ExternalException.AmbiguousTimeoutException
        case _: AuthenticationFailureException => ExternalException.AuthenticationFailureException
        case _: CouchbaseException             => ExternalException.CouchbaseException
        case _                                 => ExternalException.Unknown
      }

  def mapCleanupAttempt(
      result: TransactionCleanupAttemptEvent,
      atrEntry: Option[ActiveTransactionRecordEntry]
  ): TransactionCleanupAttempt = {
    val builder =
      com.couchbase.client.protocol.transactions.TransactionCleanupAttempt.newBuilder
        .setSuccess(result.success)
        .setAttemptId(result.attemptId)
        .addAllLogs(result.logs.asScala.map(l => l.toString).asJava)
        .setAtr(
          DocId.newBuilder
            .setBucketName(result.atrCollection.bucket)
            .setScopeName(result.atrCollection.scope.get)
            .setCollectionName(result.atrCollection.collection.get)
            .setDocId(result.atrId)
            .build
        )
    atrEntry match {
      case Some(ae) => builder.setState(mapState(ae.state))
      case None     =>
    }
    builder.build
  }
  def mapToRaise(
      toRaise: TransactionOperationFailedException.FinalErrorToRaise
  ): TransactionException = toRaise match {
    case TransactionOperationFailedException.FinalErrorToRaise.TRANSACTION_FAILED =>
      TransactionException.EXCEPTION_FAILED
    case TransactionOperationFailedException.FinalErrorToRaise.TRANSACTION_EXPIRED =>
      TransactionException.EXCEPTION_EXPIRED
    case TransactionOperationFailedException.FinalErrorToRaise.TRANSACTION_COMMIT_AMBIGUOUS =>
      TransactionException.EXCEPTION_COMMIT_AMBIGUOUS
    case TransactionOperationFailedException.FinalErrorToRaise.TRANSACTION_FAILED_POST_COMMIT =>
      TransactionException.EXCEPTION_FAILED_POST_COMMIT
    case _ =>
      throw new InternalPerformerFailure(
        new IllegalArgumentException("Unknown toRaise " + toRaise)
      )
  }
}
