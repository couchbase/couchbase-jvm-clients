package com.couchbase.client.core.retry;

import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;

import java.time.Duration;
import java.util.Optional;

public interface RetryStrategy {

  /**
   * Checks if the given request should be retried and how long the
   * retry delay should be.
   *
   * @param request the request to be checked.
   * @return If empty, no retry should be done. If a duration is returned it determines
   *         the retry delay.
   */
  Optional<Duration> shouldRetry(Request<? extends Response> request);

}
