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

package com.couchbase.client.core.util;

import java.time.Duration;

/**
 * Helper methods that have to do with certain golang-specific format the server uses.
 *
 * @since 2.0.0
 */
public class Golang {

    /**
     * Encodes a Java duration into the encoded golang format.
     *
     * @param duration the duration to encode.
     * @return the encoded duration.
     */
    public static String encodeDurationToMs(final Duration duration) {
        return duration.toMillis() + "ms";
    }
}
