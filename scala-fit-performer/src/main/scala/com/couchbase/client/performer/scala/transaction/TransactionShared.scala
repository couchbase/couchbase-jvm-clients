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
package com.couchbase.client.performer.scala.transaction

import com.couchbase.client.core.cnc.events.transaction.{IllegalDocumentStateEvent, TransactionEvent, TransactionLogEvent}
import com.couchbase.client.core.cnc.{EventSubscription, RequestSpan}
import com.couchbase.client.core.error.transaction.TransactionOperationFailedException
import com.couchbase.client.core.error.{DocumentExistsException, DocumentNotFoundException}
import com.couchbase.client.core.transaction.log.CoreTransactionLogger
import com.couchbase.client.performer.core.commands.TransactionCommandExecutor
import com.couchbase.client.performer.scala.error.InternalPerformerFailure
import com.couchbase.client.performer.scala.util.ClusterConnection
import com.couchbase.client.performer.scala.util.ResultValidation.{anythingAllowed, dbg}
import com.couchbase.client.protocol.shared.Latch
import com.couchbase.client.protocol.transactions._
import com.couchbase.client.scala.json.JsonObject
import com.couchbase.client.scala.transactions.TransactionGetResult
import com.couchbase.client.scala.transactions.error.TransactionFailedException
import com.couchbase.utils.ResultsUtil
import io.grpc.stub.StreamObserver
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}
import java.util.function.Supplier
import scala.collection.JavaConversions._
import scala.jdk.OptionConverters._
import scala.util.{Failure, Success, Try}

object TransactionShared {
  val ExpectSuccess = ExpectedResult.newBuilder.setSuccess(true).build
  private val MinimalSuccessResult =
    com.couchbase.client.protocol.transactions.TransactionResult.newBuilder
      .setException(TransactionException.NO_EXCEPTION_THROWN)
      .build

  private def waitForEvent(
      transactionEvents: ConcurrentLinkedQueue[TransactionEvent],
      e: Event
  ): Unit = {
    var found = false
    val start = System.nanoTime
    while (!found) {
      for (t <- transactionEvents) {
        if (e.getType eq EventType.EventIllegalDocumentState)
          if (t.isInstanceOf[IllegalDocumentStateEvent]) found = true
          else
            throw new InternalPerformerFailure(
              new IllegalArgumentException("Don't know how to handle event " + e.getType)
            )
      }
      try Thread.sleep(20)
      catch {
        case ex: InterruptedException =>
          ex.printStackTrace()
      }
      val now = System.nanoTime
      if (TimeUnit.NANOSECONDS.toSeconds(now - start) > 5)
        throw new TestFailure("Expected event " + e.getType + " to be raised, but it wasn't")
    }
  }

  // GRPC has a limit
  def logs(logger: CoreTransactionLogger): String = {
    val logs =
      logger.logs.toSeq.map(v => v.toString).mkString("\n")
    if (logs.length < 6000) logs else "too-big"
  }
}

abstract class TransactionShared(
    protected val executor: Option[TransactionCommandExecutor]
) {
  protected var logger: Logger      = null
  final protected val latches       = collection.mutable.Map.empty[String, CountDownLatch]
  protected var name: String        = null
  final protected val stashedGet    = new AtomicReference[TransactionGetResult]
  final protected val stashedGetMap = collection.mutable.Map.empty[Integer, TransactionGetResult]

  /**
    * There are a handful of cases where a InternalDriverFailure won't be propagated through into the final
    * TransactionFailed, e.g. if it happens while the transaction is expired.  So stash it here.
    */
  final protected val testFailure = new AtomicReference[RuntimeException]
  protected def dump(ctxLogger: CoreTransactionLogger): Unit = {
    logger.warn("Dumping logs so far for debugging:")
    ctxLogger.logs.forEach((l: TransactionLogEvent) => logger.info("    " + l.toString))
  }

  protected def runInternal(
      connection: ClusterConnection,
      req: TransactionCreateRequest,
      toTest: Option[StreamObserver[TransactionStreamPerformerToDriver]],
      performanceMode: Boolean,
      spans: collection.Map[String, RequestSpan]
  ): com.couchbase.client.scala.transactions.TransactionResult

  def run(
      connection: ClusterConnection,
      req: TransactionCreateRequest,
      toTest: Option[StreamObserver[TransactionStreamPerformerToDriver]],
      performanceMode: Boolean,
      spans: collection.Map[String, RequestSpan]
  ): com.couchbase.client.protocol.transactions.TransactionResult = {
    this.name = req.getName
    logger = LoggerFactory.getLogger(this.name)
    val transactionEvents =
      new ConcurrentLinkedQueue[TransactionEvent]
    var eventBusSubscription: EventSubscription = null
    if (!performanceMode && req.getExpectedEventsCount != 0)
      eventBusSubscription = connection.cluster.async.env.core.eventBus.subscribe {
        case ev: TransactionEvent => transactionEvents.add(ev)
        case _                    =>
      }
    try {
      val result = runInternal(connection, req, toTest, performanceMode, spans)
      if (testFailure.get != null) {
        logger.warn("Test actually failed with {}", testFailure.get.getMessage)
        throw testFailure.get
      }
      if (!performanceMode) assertExpectedEvents(req, transactionEvents)
      val cql = cleanupQueueLength(connection)
      if (performanceMode) return TransactionShared.MinimalSuccessResult
      ResultsUtil.createResult(None, result, cql)
    } catch {
      case err: TransactionFailedException =>
        if (testFailure.get != null) {
          logger.warn("Test really failed with {}", testFailure.get.toString)
          throw testFailure.get
        }
        logger.info("Error while executing an operation: {}", err.toString)
        if (!performanceMode) assertExpectedEvents(req, transactionEvents)
        val cql = cleanupQueueLength(connection)
        ResultsUtil.createResult(Some(err), Some(err.logs.toSeq), err.transactionId, false, cql)
    } finally if (eventBusSubscription != null) eventBusSubscription.unsubscribe()
  }

  private def cleanupQueueLength(connection: ClusterConnection) =
    connection.cluster.async.core.transactionsCleanup.cleanupQueueLength.asScala

  def create(req: TransactionCreateRequest): Unit = {
    this.name = req.getName
    logger = LoggerFactory.getLogger(this.name)
    req.getLatchesList.forEach((latch: Latch) => {
      logger.info("Adding new latch '{}' count={}", latch.getName, latch.getInitialCount)
      val l = new CountDownLatch(latch.getInitialCount)
      latches.put(latch.getName, l)
    })
  }

  /**
    * transactionEvents is being populated asynchronously, need to wait for the expected events to arrive.
    */
  def assertExpectedEvents(
      req: TransactionCreateRequest,
      transactionEvents: ConcurrentLinkedQueue[TransactionEvent]
  ): Unit = {
    logger.info("Waiting for all expected events")
    for (e <- req.getExpectedEventsList) {
      TransactionShared.waitForEvent(transactionEvents, e)
    }
  }

  def handleRequest(request: CommandSetLatch): Unit = {
    val latchName = request.getLatchName
    val latch     = latches(latchName)
    logger.info("Counting down latch '{}', currently is {}", latchName, latch.getCount)
    latch.countDown()
  }
  protected def handleGetOptionalResult(
      request: CommandGet,
      req: CommandGetOptional,
      out: Try[TransactionGetResult],
      cc: ClusterConnection
  ): Unit = {
    out match {
      case Failure(_: DocumentNotFoundException) =>
        if (req.getExpectDocPresent) {
          logger.info("Doc is missing but is supposed to exist")
          throw new TestFailure(new DocumentNotFoundException(null))
        } else {
          logger.info("Document missing, as expected. Allowing txn to continue.")
        }
      case Success(_) =>
        if (!req.getExpectDocPresent) {
          logger.info("Document exists but isn't supposed to")
          throw new TestFailure(new DocumentExistsException(null))
        } else {
          handleGetResult(request, out.get, cc)
        }
      case Failure(err) =>
        throw err
    }
  }

  protected def handleGetResult(
      request: CommandGet,
      getResult: TransactionGetResult,
      cc: ClusterConnection
  ): Unit = {
    stashedGet.set(getResult)
    if (request.hasStashInSlot) {
      stashedGetMap.put(request.getStashInSlot, getResult)
      logger.info("Stashed {} in slot {}", getResult.id, request.getStashInSlot)
    }
    if (request.getExpectedContentJson.nonEmpty) {
      val expected = JsonObject.fromJson(request.getExpectedContentJson)
      val actual = getResult.contentAs[JsonObject].get
      if (expected != actual) {
        //logger.warn("Expected content {}, got content {}", expected.toString, actual.toString)
        throw new TestFailure(new IllegalStateException("Did not get expected content"))
      }
    }
    if (request.getExpectedStatus != DocStatus.DO_NOT_CHECK)
      logger.warn("Ignoring request to check doc status")
  }

  protected def handleWaitOnLatch(
      request: CommandWaitOnLatch,
      txnLogger: CoreTransactionLogger
  ): Unit = {
    val latchName = request.getLatchName
    val latch     = latches(request.getLatchName)
    logger.info("Blocking on latch '{}', current count is {}", latchName, latch.getCount)
    try latch.await()
    catch {
      case e: InterruptedException =>
        e.printStackTrace()
        throw new RuntimeException(e)
    }
    logger.info("Finished blocking on latch '{}'", latchName)
    txnLogger.info("", "Finished blocking on latch '%s'", latchName)
  }

  protected def handleSetLatch(
      request: CommandSetLatch,
      toTest: StreamObserver[TransactionStreamPerformerToDriver],
      txnLogger: CoreTransactionLogger
  ): Unit = {
    val latchName = request.getLatchName
    val latch     = latches(request.getLatchName)
    logger.info("Setting own copy of latch '{}', current count is {}", latchName, latch.getCount)
    latch.countDown()
    logger.info("Telling other workers, via the driver, to set latch '{}'", latchName)
    txnLogger.info("", "Telling other performers, via the driver, to set latch '%s'", latchName)
    toTest.onNext(
      TransactionStreamPerformerToDriver.newBuilder
        .setBroadcast(BroadcastToOtherConcurrentTransactionsRequest.newBuilder.setLatchSet(request))
        .build
    )
  }

  protected def hasResult(expectedResults: Seq[ExpectedResult], er: ExpectedResult): Boolean =
    expectedResults.contains(er)

  protected def handleIfResultSucceededWhenShouldNot(
      opDebug: String,
      logger: Supplier[CoreTransactionLogger],
      expectedResults: Seq[ExpectedResult]
  ): Unit = {
    if (!hasResult(expectedResults, TransactionShared.ExpectSuccess) && !anythingAllowed(
          expectedResults
        )) {
      val l = logger.get
      val fmt = String.format(
        "Operation '%s' succeeded but was expecting %s.  Logs: %s",
        opDebug,
        dbg(expectedResults),
        TransactionShared.logs(l)
      )
      this.logger.warn(fmt)
      dump(l)
      val e = new TestFailure(fmt)
      // Make absolutely certain the test fails
      testFailure.set(e)
      throw e
    }
  }

  protected def handleOperationError(
      opDebug: String,
      dump: Runnable,
      expectedResults: Seq[ExpectedResult],
      doNotPropagateError: Boolean,
      err: RuntimeException
  ): Unit = {
    if (err.isInstanceOf[TestFailure] || err.isInstanceOf[InternalPerformerFailure]) {
      // Make absolutely certain the test fails
      testFailure.set(err)
    } else if (anythingAllowed(expectedResults)) {
    } else err match {
      case e: TransactionOperationFailedException =>
        val tofRaisedFromSDK = ErrorWrapper.newBuilder
          .setAutoRollbackAttempt(e.autoRollbackAttempt)
          .setRetryTransaction(e.retryTransaction)
          .setToRaise(ResultsUtil.mapToRaise(e.toRaise))
        val causeFromTOF = ResultsUtil.mapCause(e.getCause)
        var ok = false
        for (er <- expectedResults) {
          if (er.hasError) {
            val anExpectedError = er.getError
            // Note we no longer compare error classes
            if (anExpectedError.getAutoRollbackAttempt == tofRaisedFromSDK.getAutoRollbackAttempt && anExpectedError.getRetryTransaction == tofRaisedFromSDK.getRetryTransaction && (anExpectedError.getToRaise eq tofRaisedFromSDK.getToRaise)) {
              val c = anExpectedError.getCause
              if (c.getDoNotCheck || (c.hasException && c.getException == causeFromTOF)) ok = true
            }
          }
        }
        if (!ok) {
          val fmt = String.format(
            "Operation '%s' failed unexpectedly, was expecting %s but got %s: %s",
            opDebug,
            dbg(expectedResults),
            tofRaisedFromSDK,
            err.getMessage
          )
          logger.warn(fmt)
          dump.run()
          val error = new TestFailure(fmt, err)
          // Make absolutely certain the test fails
          testFailure.set(error)
          throw error
        } else
          logger.info(
            "Operation '{}' failed with {} as expected: {}",
            opDebug,
            tofRaisedFromSDK.build,
            err.getMessage
          )
      case _ =>
        var ok = false
        for (er <- expectedResults) {
          if (er.hasException) {
            val raisedFromSDK = ResultsUtil.mapCause(err)
            val expected = er.getException

            if ((expected ne ExternalException.Unknown) && raisedFromSDK == expected) {
              ok = true
            }
          }
        }
        if (!ok) {
          val msg = String.format(
            "Operation '%s' failed unexpectedly, was expecting %s but got %s: %s",
            opDebug,
            dbg(expectedResults),
            err,
            err.getMessage
          )
          logger.warn(msg)
          dump.run()
          val error = new InternalPerformerFailure(
            new IllegalStateException(msg)
          )
          // Make absolutely certain the test fails
          testFailure.set(error)
          throw error
        }
    }
    if (doNotPropagateError) {
      val msg = "Not propagating the error, as requested by test"
      logger.info(msg)
    } else throw err
  }
}
