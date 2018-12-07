/*
 * Copyright (c) 2018 Couchbase, Inc.
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

package com.couchbase.client.java.query;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.msg.query.QueryResponse;

import java.util.function.Consumer;

public class AsyncQueryResult implements QueryResponse.QueryEventSubscriber {

  private final Consumer<QueryRow> rowConsumer;
  private volatile boolean complete;
  private volatile QueryResponse response;

  public AsyncQueryResult(final Consumer<QueryRow> rowConsumer) {
    this.rowConsumer = rowConsumer;
    complete = false;
  }

  @Stability.Internal
  public void result(final QueryResponse response) {
    this.response = response;
  }

  @Override
  public void onNext(final QueryResponse.QueryEvent event) {
    if (event.rowType() == QueryResponse.QueryEventType.ROW) {
      rowConsumer.accept(new QueryRow(event.data()));
    }
  }

  public void request(long num) {
    response.request(num);
  }

  // TODO: I assume cancellation needs to be done THROUGH THE request cancellation
  // mechanism to be consistent?
  public void cancel() {
    response.cancel();
  }

  /**
   * Returns true if this result is completed.
   *
   * @return true once completed.
   */
  public boolean completed() {
    return complete;
  }

  @Override
  public void onComplete() {
    complete = true;
  }

}
