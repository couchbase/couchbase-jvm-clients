/*
 * Copyright (c) 2019 Couchbase, Inc.
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

package com.couchbase.client.core.cnc;

import com.couchbase.client.core.annotation.Stability;

/**
 * Marker interface for external spans that can be passed in into the SDK option blocks.
 * <p>
 * Note that you'll most likely consume this interface through actual implementations from the tracer module that
 * is used for your application. You will not need to worry about this with the default logging tracer, but if you
 * are using OpenTracing or OpenTelemetry, look in their respective codebases for implementations of this class.
 */
@Stability.Volatile
public interface RequestSpan {
}
