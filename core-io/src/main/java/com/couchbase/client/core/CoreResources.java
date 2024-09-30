/*
 * Copyright (c) 2024 Couchbase, Inc.
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
package com.couchbase.client.core;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.CoreCouchbaseOps;
import com.couchbase.client.core.cnc.RequestTracer;

/**
 * Resources that are owned by a {@link CoreCouchbaseOps}.  (E.g. either a {@link Core} or {@link CoreProtostellar}.
 * <p>
 * It is explicitly not owned by a CoreEnvironment, which can be shared between multiple Cluster objects, and so is not suitable for any information
 * tied to a CoreCouchbaseOps.
 * <p>
 * Consider preferring adding new resources here rather than into the *Environment objects.
 */
@Stability.Internal
public interface CoreResources {
  RequestTracer requestTracer();
}
