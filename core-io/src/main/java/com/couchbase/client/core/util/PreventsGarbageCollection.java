/*
 * Copyright 2023 Couchbase, Inc.
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

package com.couchbase.client.core.util;

import com.couchbase.client.core.annotation.Stability;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Indicates that at least one purpose of the annotated field
 * is to prevent the value from being garbage collected.
 * <p>
 * This annotation is strictly advisory, and has no effect at runtime.
 * <p>
 * Why do we hold references just to prevent garbage collection?
 * One reason is because we want to automatically disconnect an AsyncCluster
 * when it becomes unreachable, so the Core doesn't live on as
 * a socket-slurping zombie. However, we don't want
 * to disconnect the cluster until any Buckets, Scopes, Collections,
 * and Managers associated with the cluster also become unreachable.
 * To that end, each of those "satellite" objects holds a reference
 * back to the cluster, just to prevent the cluster from becoming
 * unreachable until all the satellites are also unreachable.
 * <p>
 * We explored attaching the "auto-disconnect" behavior to the
 * Core instead of the cluster, but unfortunately the Core
 * never becomes eligible for garbage collection, presumably
 * because it's referenced by one or more schedulers.
 */
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.FIELD)
@Stability.Internal
public @interface PreventsGarbageCollection {
}
