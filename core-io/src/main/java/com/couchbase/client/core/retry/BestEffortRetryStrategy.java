package com.couchbase.client.core.retry;

import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;

import java.time.Duration;
import java.util.Optional;

public class BestEffortRetryStrategy implements RetryStrategy {

  public static BestEffortRetryStrategy INSTANCE = new BestEffortRetryStrategy();

  private BestEffortRetryStrategy() { }

  @Override
  public Optional<Duration> shouldRetry(final Request<? extends Response> request) {
    // todo: fixme with configurable, default delay -- exponential?
    return Optional.of(Duration.ofMillis(100));
  }

  @Override
  public String toString() {
    return "BestEffort";
  }

}
