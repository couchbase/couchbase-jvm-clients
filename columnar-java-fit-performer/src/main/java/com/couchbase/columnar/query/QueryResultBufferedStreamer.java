/*
 * Copyright (c) 2024 Couchbase, Inc.
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

package com.couchbase.columnar.query;

import com.couchbase.columnar.client.java.QueryResult;
import com.couchbase.columnar.client.java.Queryable;
import com.couchbase.columnar.client.java.Row;
import com.couchbase.columnar.fit.core.util.ResultUtil;
import fit.columnar.EmptyResultOrFailureResponse;
import fit.columnar.ExecuteQueryRequest;
import fit.columnar.QueryResultMetadataResponse;
import fit.columnar.QueryRowResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.couchbase.columnar.query.PushBasedStreamer.toFit;

class BufferedStreamer extends Thread {
  private final Logger logger;
  private final BlockingQueue<QueryRowResponse> rows = new LinkedBlockingQueue<>(1);
  private final BlockingQueue<fit.columnar.EmptyResultOrFailureResponse> queryResult = new LinkedBlockingQueue<>(1);
  private final CompletableFuture<fit.columnar.QueryResultMetadataResponse> metadata = new CompletableFuture<>();
  private final AtomicBoolean rowsFinished = new AtomicBoolean(false);
  private final Queryable clusterOrScope;
  private final ExecuteQueryRequest executeQueryRequest;
  private final String queryHandle;

  public BufferedStreamer(Queryable clusterOrScope, ExecuteQueryRequest request, String queryHandle) {
    this.logger = LoggerFactory.getLogger("Query " + queryHandle);
    this.clusterOrScope = clusterOrScope;
    this.executeQueryRequest = request;
    this.queryHandle = queryHandle;
  }

  public fit.columnar.QueryResultMetadataResponse blockForMetadata() {
    try {
      return metadata.get(30, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      throw new RuntimeException(e.getCause());
    } catch (InterruptedException | TimeoutException e) {
      throw new RuntimeException(e);
    }
  }

  public void cancel() {
    logger.info("Cancelling query");
    // This is how a user cancels operations in this SDK: using the JVM interrupt mechanism.
    interrupt();
  }



  @Override
  public void run() {
      logger.info("Starting buffered query on handle {}: {}", queryHandle, executeQueryRequest.getStatement());

      try {
        QueryResult result;

        try {
          var options = QueryOptionsUtil.convertQueryOptions(executeQueryRequest);
          result = options != null
            ? clusterOrScope.executeQuery(executeQueryRequest.getStatement(), options)
            : clusterOrScope.executeQuery(executeQueryRequest.getStatement());

        } catch (RuntimeException err) {
          logger.info("executeQuery failed at initial point: {}", err.toString());
          // todo think about timings
          queryResult.put(ResultUtil.failure(err, null));
          // End thread
          throw err;
        }

        logger.info("executeQuery() succeeded, proceeding with row iteration");

        // todo think about timings
        queryResult.put(ResultUtil.success(null));

        for (Row next : result.rows()) {
          var processed = QueryRowUtil.processRow(executeQueryRequest, next);
          // This will block until the driver pulls it
          rows.put(processed.row());
        }

        logger.info("Finished row iteration");
        rowsFinished.set(true);

        metadata.complete(fit.columnar.QueryResultMetadataResponse.newBuilder()
          .setSuccess(toFit(result.metadata()))
          .build());
      }
      catch (RuntimeException | InterruptedException err) {
        logger.warn("executeQuery thread failed unexpectedly: " + err);
      }
  }

  public fit.columnar.QueryRowResponse blockForRow() {
    try {
      logger.info("Waiting for a row to be available");

      if (rowsFinished.get()) {
        if (!rows.isEmpty()) {
          return rows.take();
        }

        logger.info("Row iteration is complete");

        return fit.columnar.QueryRowResponse.newBuilder()
                .setSuccess(fit.columnar.QueryRowResponse.Result.newBuilder()
                        .setEndOfStream(true))
                .build();
      }

      return rows.take();
    } catch (InterruptedException e) {
      logger.info("Interrupted taking row!");
      throw new RuntimeException(e);
    }
  }

  public EmptyResultOrFailureResponse blockForQueryResult() {
    logger.info("Waiting for QueryResult available");

    try {
      return queryResult.take();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}

// Handles a streaming query result.
public class QueryResultBufferedStreamer implements ExecuteQueryStreamer {
  private final String queryHandle = UUID.randomUUID().toString().substring(0, 6);
  private final BufferedStreamer asyncExecuteQuery;

  public QueryResultBufferedStreamer(Queryable clusterOrScope, ExecuteQueryRequest request) {
    asyncExecuteQuery = new BufferedStreamer(clusterOrScope, request, queryHandle);
    asyncExecuteQuery.start();
  }

  @Override
  public EmptyResultOrFailureResponse blockForQueryResult() {
    return asyncExecuteQuery.blockForQueryResult();
  }

  public QueryRowResponse blockForRow() {
    return asyncExecuteQuery.blockForRow();
  }

  public QueryResultMetadataResponse blockForMetadata() {
    return asyncExecuteQuery.blockForMetadata();
  }

  public void cancel() {
    asyncExecuteQuery.cancel();
  }

  public String queryHandle() {
    return queryHandle;
  }
}
