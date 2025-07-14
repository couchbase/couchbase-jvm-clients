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

import com.couchbase.client.core.cnc.RequestSpan
import com.couchbase.client.performer.scala.error.InternalPerformerFailure
import com.couchbase.client.performer.scala.util.ClusterConnection
import com.couchbase.client.protocol.shared.API
import com.couchbase.client.protocol.transactions.{
  TransactionCreated,
  TransactionStreamDriverToPerformer,
  TransactionStreamPerformerToDriver
}
import io.grpc.stub.StreamObserver
import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.AtomicReference

class TransactionMarshaller(
    private val clusterConnections: collection.Map[String, ClusterConnection],
    private val spans: collection.Map[String, RequestSpan]
) {
  final private val thread = new AtomicReference[Thread]
  final private val logger = LoggerFactory.getLogger(classOf[TransactionMarshaller])
  private var fromTest: StreamObserver[TransactionStreamDriverToPerformer] = null
  private var twoWay: TransactionShared                                    = null
  @volatile private var readyToStart                                       = false

  private def shutdown(): Unit = {
    logger.info("Shutting down")
    val t = thread.get
    if (t != null)
      try t.join()
      catch {
        case e: InterruptedException =>
          e.printStackTrace()
      }
    logger.info("Finished shutting down")
  }

  def run(
      toTest: StreamObserver[TransactionStreamPerformerToDriver]
  ): StreamObserver[TransactionStreamDriverToPerformer] = {
    fromTest = new StreamObserver[TransactionStreamDriverToPerformer]() {
      override def onNext(next: TransactionStreamDriverToPerformer): Unit = {
        logger.info("From driver: {}", next.toString.trim)
        if (next.hasCreate) {
          val req = next.getCreate
          val bp  = req.getName + ": "
          val t   = new Thread(() => {
            if (req.getApi eq API.DEFAULT) twoWay = new TransactionBlocking(null)
            else throw new UnsupportedOperationException("Only default API currently supported")
            twoWay.create(req)
            toTest.onNext(
              TransactionStreamPerformerToDriver.newBuilder
                .setCreated(TransactionCreated.newBuilder.build)
                .build
            )
            logger.info("{}Created, waiting until told to start", bp)
            while (!readyToStart)
              try Thread.sleep(50)
              catch {
                case e: InterruptedException =>
                  e.printStackTrace()
              }
            logger.info("{}Starting", bp)
            val cc     = clusterConnections(req.getClusterConnectionId)
            val result = twoWay.run(cc, req, Some(toTest), false, spans)
            logger.info("Transaction has finished, completing stream and ending thread")
            toTest.onNext(
              TransactionStreamPerformerToDriver.newBuilder.setFinalResult(result).build
            )
            toTest.onCompleted()

          })
          t.start()
          thread.set(t)
        } else if (next.hasStart) readyToStart = true
        else if (next.hasBroadcast) {
          val req = next.getBroadcast
          if (req.hasLatchSet) {
            // Txn is likely blocked on a latch so this can't be put on the queue for it
            val request = req.getLatchSet
            twoWay.handleRequest(request)
          } else
            throw new InternalPerformerFailure(
              new IllegalStateException("Unknown broadcast request from driver " + fromTest)
            )
        } else
          throw new InternalPerformerFailure(
            new IllegalStateException("Unknown request from driver " + next)
          )
      }

      override def onError(throwable: Throwable): Unit = {
        logger.error("From driver: {}", throwable.getMessage)
        shutdown()
      }

      override def onCompleted(): Unit = {
        logger.error("From driver: complete")
        shutdown()
      }
    }
    fromTest
  }
}
