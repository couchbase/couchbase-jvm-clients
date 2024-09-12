package com.couchbase.columnar.util;

import com.couchbase.columnar.fit.core.util.StartTimes;
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
