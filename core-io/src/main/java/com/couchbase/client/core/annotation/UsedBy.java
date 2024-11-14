/*
 * Copyright (c) 2023 Couchbase, Inc.
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
package com.couchbase.client.core.annotation;

import java.lang.annotation.Documented;

/**
 * Indicates the target element is not intended for public use,
 * but some other Couchbase project depends on it.
 * Changes to the API should be coordinated with the other project.
 * <p>
 * The target element should also be annotated with {@link Stability.Internal}.
 * <p>
 * Should be used sparingly, and only after deciding we don't want
 * to promote the target element to the public API.
 */
@Documented
@Stability.Internal // The annotation itself is internal API
public @interface UsedBy {
  Project value();

  enum Project {
    SPRING_DATA_COUCHBASE,
    QUARKUS_COUCHBASE
  }
}
