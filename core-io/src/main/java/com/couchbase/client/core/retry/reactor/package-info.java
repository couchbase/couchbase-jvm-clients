/*
 * Copyright 2025 Couchbase, Inc.
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
 * This package consists of code borrowed from an old version of the optional
 * `reactor-extra` library. Modern versions of Reactor have incorporated
 * retry functionality into the core library. New code should not use
 * classes in this package.
 */
@NullUnmarked // to minimize drift from original third-party code
package com.couchbase.client.core.retry.reactor;

import org.jspecify.annotations.NullUnmarked;
