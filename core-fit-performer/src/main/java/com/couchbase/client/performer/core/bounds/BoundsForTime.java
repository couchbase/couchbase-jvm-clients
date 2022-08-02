/*
 * Copyright (c) 2022 Couchbase, Inc.
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
package com.couchbase.client.performer.core.bounds;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class BoundsForTime implements BoundsExecutor {
    private final long start = System.nanoTime();
    private final Duration untilSeconds;

    public BoundsForTime(Duration untilSeconds) {
        this.untilSeconds = untilSeconds;
    }

    @Override
    public boolean canExecute() {
        return TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start) < untilSeconds.toSeconds();
    }
}
