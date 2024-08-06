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
package com.couchbase.columnar.fit.core.util;

import fit.columnar.EmptyResultOrFailureResponse;
import fit.columnar.ResponseMetadata;

public class ResultUtil {
  public static EmptyResultOrFailureResponse success(StartTimes startTime) {
    return fit.columnar.EmptyResultOrFailureResponse.newBuilder()
            .setEmptySuccess(true)
            .setMetadata(responseMetadata(startTime))
            .build();
  }

  public static EmptyResultOrFailureResponse failure(Throwable err, StartTimes startTime) {
    return fit.columnar.EmptyResultOrFailureResponse.newBuilder()
            .setMetadata(responseMetadata(startTime))
            .setError(ErrorUtil.convertError(err))
            .build();
  }

  public static ResponseMetadata responseMetadata(StartTimes startTime) {
    if (startTime != null) {
      return fit.columnar.ResponseMetadata.newBuilder()
              .setElapsedNanos(System.nanoTime() - startTime.asSystem())
              .setInitiated(startTime.asWallclock())
              .build();
    }
    else {
      // todo remove when fix timings
      return fit.columnar.ResponseMetadata.newBuilder().build();
    }
  }
}
