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
package com.couchbase.client.core.topology;

import com.couchbase.client.core.config.ClusterConfig;
import reactor.util.annotation.Nullable;

public class ClusterIdentifierUtil {
  private ClusterIdentifierUtil() {}

  public static @Nullable ClusterIdentifier fromConfig(@Nullable ClusterConfig config) {
    return config == null ? null : config.globalConfig() == null ? null : config.globalConfig().clusterIdent();
  }
}
