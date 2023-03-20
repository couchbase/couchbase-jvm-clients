/*
 * Copyright (c) 2020 Couchbase, Inc.
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
// [skip:<3.3.0]
package com.couchbase.twoway;

import com.couchbase.InternalPerformerFailure;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.protocol.shared.API;
import com.couchbase.client.protocol.transactions.BroadcastToOtherConcurrentTransactionsRequest;
import com.couchbase.client.protocol.transactions.CommandSetLatch;
import com.couchbase.client.protocol.transactions.TransactionCreateRequest;
import com.couchbase.client.protocol.transactions.TransactionCreated;
import com.couchbase.client.protocol.transactions.TransactionResult;
import com.couchbase.client.protocol.transactions.TransactionStreamDriverToPerformer;
import com.couchbase.client.protocol.transactions.TransactionStreamPerformerToDriver;
import com.couchbase.utils.ClusterConnection;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Wraps around TwoWayTransaction and provides some of the complex stuff, like turning requests from
 * the driver into something more manageable, and handling threads.
 *
 * This allows TwoWayTransaction to be used independently as a simple low-overhead threadless test executor when
 * desired.
 */
public class TwoWayTransactionMarshaller {
    private final AtomicReference<Thread> thread = new AtomicReference<>();
    private final Logger logger = LoggerFactory.getLogger(TwoWayTransactionMarshaller.class);
    private StreamObserver<TransactionStreamDriverToPerformer> fromTest;
    private final ConcurrentHashMap<String, ClusterConnection> clusterConnections;
    private TwoWayTransactionShared twoWay;
    private volatile boolean readyToStart = false;
    private final ConcurrentHashMap<String, RequestSpan> spans;

    public TwoWayTransactionMarshaller(ConcurrentHashMap<String, ClusterConnection> clusterConnections,
                                       ConcurrentHashMap<String, RequestSpan> spans) {
        this.clusterConnections = clusterConnections;
        this.spans = spans;
    }

    private void shutdown() {
        logger.info("Shutting down");
        Thread t = thread.get();
        if (t != null) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        logger.info("Finished shutting down");
    }

    public StreamObserver<TransactionStreamDriverToPerformer> run(StreamObserver<TransactionStreamPerformerToDriver> toTest) {
        fromTest = new StreamObserver<TransactionStreamDriverToPerformer>() {
            @Override
            public void onNext(TransactionStreamDriverToPerformer next) {
                logger.info("From driver: {}", next.toString().trim());

                if (next.hasCreate()) {
                    final TransactionCreateRequest req = next.getCreate();
                    final String bp = req.getName() + ": ";

                    Thread t = new Thread(() -> {
                        if (req.getApi() == API.DEFAULT) {
                            twoWay = new TwoWayTransactionBlocking(null);
                        }
                        else {
                            twoWay = new TwoWayTransactionReactive();
                        }
                        twoWay.create(req);

                        toTest.onNext(TransactionStreamPerformerToDriver.newBuilder()
                            .setCreated(TransactionCreated.newBuilder().build())
                            .build());

                        logger.info("{}Created, waiting until told to start", bp);

                        while (!readyToStart) {
                            try {
                                Thread.sleep(50);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }

                        logger.info("{}Starting", bp);

                        var cc = clusterConnections.get(req.getClusterConnectionId());

                        TransactionResult result = twoWay.run(clusterConnections.get(req.getClusterConnectionId()),
                                req,
                                toTest,
                                false,
                                spans);

                        logger.info("Transaction has finished, completing stream and ending thread");

                        toTest.onNext(TransactionStreamPerformerToDriver.newBuilder()
                                .setFinalResult(result)
                                .build());
                        toTest.onCompleted();

                    });

                    t.start();
                    thread.set(t);
                }
                else if (next.hasStart()) {
                    readyToStart = true;
                }
                else if (next.hasBroadcast()) {
                    BroadcastToOtherConcurrentTransactionsRequest req = next.getBroadcast();

                    if (req.hasLatchSet()) {
                        // Txn is likely blocked on a latch so this can't be put on the queue for it
                        CommandSetLatch request = req.getLatchSet();
                        twoWay.handleRequest(request);
                    } else {
                        throw new InternalPerformerFailure(
                            new IllegalStateException("Unknown broadcast request from driver " + fromTest));
                    }

                }
                else {
                    throw new InternalPerformerFailure(
                            new IllegalStateException("Unknown request from driver " + next));
                }
            }

            @Override
            public void onError(Throwable throwable) {
                logger.error("From driver: {}", throwable.getMessage());
                shutdown();
            }

            @Override
            public void onCompleted() {
                logger.error("From driver: complete");
                shutdown();
            }
        };

        return fromTest;
    }
}