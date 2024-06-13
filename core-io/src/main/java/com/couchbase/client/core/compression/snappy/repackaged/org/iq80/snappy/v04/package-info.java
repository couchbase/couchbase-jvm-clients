/*
 * Copyright 2024 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * These classes came from https://github.com/dain/snappy/ at tag "snappy-0.4".
 * <p>
 * They have been modified to always use "SlowMemory" instead of "UnsafeMemory",
 * to avoid CVE-2024-36124.
 */
package com.couchbase.client.core.compression.snappy.repackaged.org.iq80.snappy.v04;
