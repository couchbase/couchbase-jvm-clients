/*
 * Copyright 2026 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.core.transaction.util;

import com.couchbase.client.core.annotation.Stability;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

@Stability.Internal
public class BlockingRetryHandler {
    private static final double EXPONENTIAL_MULTIPLER = 2.0;

    private final Duration maxBackoff;
    private final Optional<Double> jitter;
    private final Optional<Integer> maxRetries;
    private final Optional<Duration> maxDuration;
    private final long startNanos;

    private boolean shouldRetry = false;
    private Duration nextBackoff;
    private int currentRetries = 0;

    public static Builder builder(Duration initialBackoff, Duration maxBackoff) {
        return new Builder(initialBackoff, maxBackoff);
    }

    private BlockingRetryHandler(Duration initialBackoff,
                                 Duration maxBackoff,
                                 Optional<Double> jitter,
                                 Optional<Integer> maxRetries,
                                 Optional<Duration> maxDuration) {
        this.maxBackoff = maxBackoff;
        this.jitter = jitter;
        this.maxRetries = maxRetries;
        this.maxDuration = maxDuration;
        this.startNanos = System.nanoTime();
        this.nextBackoff = initialBackoff;
    }

    public void shouldRetry(boolean shouldRetry) {
        this.shouldRetry = shouldRetry;
    }

    public boolean shouldRetry() {
        return shouldRetry;
    }

    private void doSleep(Duration delay) {
        try {
            NANOSECONDS.sleep(delay.toNanos());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while backing off", e);
        }
    }

    public void sleepForNextBackoffAndSetShouldRetry() {
        if (hasExceededMaxDuration()) {
            // This overrides the request
            shouldRetry(false);
            return;
        }
        currentRetries++;
        if (maxRetries.isPresent() && currentRetries > maxRetries.get()) {
            shouldRetry(false);
            return;
        }
        shouldRetry(true);
        doSleep(calcNextBackoff());
    }

    public void sleepForFixedIntervalAndSetShouldRetry(Duration fixedInterval) {
        Objects.requireNonNull(fixedInterval, "fixedInterval");
        if (hasExceededMaxDuration()) {
            // This overrides the request
            shouldRetry(false);
            return;
        }
        currentRetries++;
        if (maxRetries.isPresent() && currentRetries > maxRetries.get()) {
            shouldRetry(false);
            return;
        }
        shouldRetry(true);
        doSleep(applyJitter(fixedInterval));
    }

    public Duration peekNextBackoff() {
        return nextBackoff;
    }

    // For tests
    public Duration calcNextBackoff() {
        Duration current = nextBackoff;
        long currentNanos = current.toNanos();
        long nextNanos = (long) (currentNanos * EXPONENTIAL_MULTIPLER);
        Duration next = Duration.ofNanos(Math.min(nextNanos, maxBackoff.toNanos()));
        Duration withJitter = applyJitter(current);
        nextBackoff = next;
        return withJitter;
    }

    private Duration applyJitter(Duration duration) {
        return jitter.map(j -> {
            long nanos = duration.toNanos();
            long jitterNanos = (long) (nanos * j * ThreadLocalRandom.current().nextDouble(-1.0, 1.0));
            long result = nanos + jitterNanos;
            return Duration.ofNanos(Math.max(0, Math.min(result, maxBackoff.toNanos())));
        }).orElse(duration);
    }

    private boolean hasExceededMaxDuration() {
        if (!maxDuration.isPresent()) {
            return false;
        }
        long elapsedNanos = System.nanoTime() - startNanos;
        return Duration.ofNanos(elapsedNanos).compareTo(maxDuration.get()) > 0;
    }

    public static class Builder {
        private final Duration initialBackoff;
        private final Duration maxBackoff;
        private Optional<Double> jitter = Optional.empty();
        private Optional<Integer> maxRetries = Optional.empty();
        private Optional<Duration> maxDuration = Optional.empty();

        private Builder(Duration initialBackoff, Duration maxBackoff) {
            this.initialBackoff = Objects.requireNonNull(initialBackoff, "initialBackoff");
            this.maxBackoff = Objects.requireNonNull(maxBackoff, "maxBackoff");
        }

        public Builder jitter(double jitter) {
            this.jitter = Optional.of(jitter);
            return this;
        }

        public Builder maxRetries(int maxRetries) {
            this.maxRetries = Optional.of(maxRetries);
            return this;
        }

        public Builder maxDuration(Duration maxDuration) {
            this.maxDuration = Optional.of(Objects.requireNonNull(maxDuration, "maxDuration"));
            return this;
        }

        public BlockingRetryHandler build() {
            validate();
            return new BlockingRetryHandler(initialBackoff, maxBackoff, jitter, maxRetries, maxDuration);
        }

        private void validate() {
            if (initialBackoff.isZero() || initialBackoff.isNegative()) {
                throw new IllegalArgumentException("initialBackoff must be positive");
            }
            if (maxBackoff.isZero() || maxBackoff.isNegative()) {
                throw new IllegalArgumentException("maxBackoff must be positive");
            }
            if (maxBackoff.compareTo(initialBackoff) < 0) {
                throw new IllegalArgumentException("maxBackoff must be >= initialBackoff");
            }
            jitter.ifPresent(j -> {
                if (j < 0.0d || j > 1.0d) {
                    throw new IllegalArgumentException("jitter must be between 0.0 and 1.0");
                }
            });
            maxRetries.ifPresent(r -> {
                if (r < 0) {
                    throw new IllegalArgumentException("maxRetries must be >= 0");
                }
            });
            maxDuration.ifPresent(d -> {
                if (d.isZero() || d.isNegative()) {
                    throw new IllegalArgumentException("maxDuration must be positive");
                }
            });
        }
    }
}
