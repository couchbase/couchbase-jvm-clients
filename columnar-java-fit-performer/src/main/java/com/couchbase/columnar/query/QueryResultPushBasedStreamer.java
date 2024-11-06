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

import com.couchbase.columnar.client.java.QueryMetadata;
import com.couchbase.columnar.client.java.QueryOptions;
import com.couchbase.columnar.client.java.Queryable;
import com.couchbase.columnar.client.java.Row;
import com.couchbase.columnar.client.java.json.JsonArray;
import com.couchbase.columnar.client.java.json.JsonObject;
import com.couchbase.columnar.content.ContentAsUtil;
import com.couchbase.columnar.util.ErrorUtil;
import fit.columnar.EmptyResultOrFailureResponse;
import fit.columnar.ExecuteQueryRequest;
import fit.columnar.QueryResultMetadataResponse;
import fit.columnar.QueryRowResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

import static com.couchbase.columnar.query.QueryOptionsUtil.convertQueryOptions;

class PushBasedStreamer extends Thread {
  private final Logger logger;
  private final LinkedBlockingQueue<fit.columnar.QueryRowResponse> rows = new LinkedBlockingQueue<>(1);


  private final LinkedBlockingQueue<fit.columnar.QueryResultMetadataResponse> queryResult = new LinkedBlockingQueue<>(1);
  private final Queryable clusterOrScope;
  private final ExecuteQueryRequest executeQueryRequest;
  private final String queryHandle;

  public PushBasedStreamer(Queryable clusterOrScope, ExecuteQueryRequest request, String queryHandle) {
    this.logger = LoggerFactory.getLogger("Query " + queryHandle);
    this.clusterOrScope = clusterOrScope;
    this.executeQueryRequest = request;
    this.queryHandle = queryHandle;
  }

  public QueryResultMetadataResponse blockForMetadata() {
    try {
      logger.info("Blocking until metadata available");
      var out = queryResult.take();
      logger.info("Metadata present");
      return out;
    } catch (InterruptedException e) {
      logger.info("Interrupted getting metadata!");
      throw new RuntimeException(e);
    }
  }

  public void cancel() {
    logger.info("Cancelling query");
    // This is how a user cancels operations in this SDK: using the JVM interrupt mechanism.
    interrupt();
  }

  record RowProcessingResult(@Nullable Throwable errorThrown, QueryRowResponse row) {}

  @Override
  public void run() {
    try {
      logger.info("Starting query on handle {}: {}", queryHandle, executeQueryRequest.getStatement());

      final QueryMetadata metadata;

      try {
        Consumer<QueryOptions> optionsCallback = convertQueryOptions(executeQueryRequest);
        metadata = optionsCallback != null
          ? clusterOrScope.executeStreamingQuery(executeQueryRequest.getStatement(), this::handleRow, optionsCallback)
          : clusterOrScope.executeStreamingQuery(executeQueryRequest.getStatement(), this::handleRow);
      } catch (RuntimeException err) {
        logger.info("executeQuery failed at initial point", err);
        queryResult.put(QueryResultMetadataResponse.newBuilder()
          .setFailure(ErrorUtil.convertError(err))
          .build());
        // End thread
        throw err;
      }

      try {
        rows.put(QueryRowResponse.newBuilder()
          // todo metadata
//                .setMetadata()
          .setSuccess(QueryRowResponse.Result.newBuilder()
            .setEndOfStream(true))
          .build());
      } catch (InterruptedException e) {
        logger.info("Interrupted putting final row!");
      }

      logger.info("Reached end: {}", metadata);

      queryResult.put(fit.columnar.QueryResultMetadataResponse.newBuilder()
        .setSuccess(toFit(metadata))
        .build());
    } catch (Throwable err) {
      logger.info("executeQuery failed with {}", err.toString());
    }

    logger.info("executeQuery end of thread");

  }

  static QueryResultMetadataResponse.QueryMetadata toFit(QueryMetadata sdk) {
    var metrics = sdk.metrics();
    return fit.columnar.QueryResultMetadataResponse.QueryMetadata.newBuilder()
      .setMetrics(fit.columnar.QueryResultMetadataResponse.QueryMetadata.Metrics.newBuilder()
        .setElapsedTime(com.google.protobuf.Duration.newBuilder().setSeconds(metrics.elapsedTime().getSeconds()))
        .setExecutionTime(com.google.protobuf.Duration.newBuilder().setSeconds(metrics.executionTime().getSeconds()))
        .setResultCount(metrics.resultCount())
        .setResultSize(metrics.resultSize())
        .setProcessedObjects(metrics.processedObjects())
      )
      .build();
  }

  private void handleRow(Row row) {
    try {
      logger.info("Got row");

      var next = processRow(row);
      rows.put(next.row);
      logger.info("Done putting row");
      if (next.errorThrown != null) {
        logger.info("Row threw error so throwing that out to the SDK");
      }

    } catch (InterruptedException e) {
      logger.info("Interrupted putting row!");
      throw new RuntimeException(e);
    }
  }

  private RowProcessingResult processRow(Row row) {
    if (executeQueryRequest.hasContentAs()) {
      var content = ContentAsUtil.contentType(
        executeQueryRequest.getContentAs(),
        () -> row.bytes(),
        () -> row.asNullable(JsonArray.class),
        () -> row.asNullable(JsonObject.class),
        () -> row.asNullable(String.class)
      );

      if (content.isSuccess()) {
        return new RowProcessingResult(null, fit.columnar.QueryRowResponse.newBuilder()
          // todo metadata
//                .setMetadata()
          .setSuccess(fit.columnar.QueryRowResponse.Result.newBuilder()
            .setRow(fit.columnar.QueryRowResponse.Row.newBuilder()
              .setRowContent(content.value())))
          .build());
      } else {
        return new RowProcessingResult(content.exception(), fit.columnar.QueryRowResponse.newBuilder()
          // todo metadata
//                .setMetadata()
          .setRowLevelFailure(ErrorUtil.convertError(content.exception()))
          .build());
      }
    } else {
      return new RowProcessingResult(null, fit.columnar.QueryRowResponse.newBuilder()
        // todo metadata
//                .setMetadata()
        .setSuccess(fit.columnar.QueryRowResponse.Result.getDefaultInstance()).build());
    }
  }

  public QueryRowResponse blockForRow() {
    try {
      logger.info("Waiting for a row to be available");
      while (true) {
        if (rows.isEmpty()) {
          if (!queryResult.isEmpty() && queryResult.peek().hasFailure()) {
            logger.info("ExecuteQuery failed while waiting for a row. Error was: {}", queryResult.peek().getFailure());
            return QueryRowResponse.newBuilder()
              // todo metadata
//                .setMetadata()
              .setExecuteQueryFailure(queryResult.peek().getFailure())
              .build();
          }
          Thread.sleep(10);
        } else {
          break;
        }
      }

      logger.info("Row is available");
      return rows.take();
    } catch (InterruptedException e) {
      logger.info("Interrupted taking row!");
      throw new RuntimeException(e);
    }
  }
}

// Handles a streaming query result.
public class QueryResultPushBasedStreamer implements ExecuteQueryStreamer {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryResultPushBasedStreamer.class);

  private final String queryHandle = UUID.randomUUID().toString().substring(0, 6);
  private final PushBasedStreamer asyncExecuteQuery;

  public QueryResultPushBasedStreamer(Queryable clusterOrScope, ExecuteQueryRequest request) {
    asyncExecuteQuery = new PushBasedStreamer(clusterOrScope, request, queryHandle);
    asyncExecuteQuery.start();
  }

  @Override
  public EmptyResultOrFailureResponse blockForQueryResult() {
    throw new UnsupportedOperationException("This mode does not have QueryResult and the driver should not have asked for it");
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
