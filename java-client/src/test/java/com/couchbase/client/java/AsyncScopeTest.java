/*
 * Copyright (c) 2020 Couchbase, Inc.
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

package com.couchbase.client.java;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.config.ConfigurationProvider;
import com.couchbase.client.java.env.ClusterEnvironment;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AsyncScopeTest {

  @Test
  @SuppressWarnings("unchecked")
  void shouldReuseAsyncCollection() {
    Core core = mock(Core.class);
    ConfigurationProvider configProvider = mock(ConfigurationProvider.class);
    Flux<ClusterConfig> configs = (Flux<ClusterConfig>) mock(Flux.class);
    when(configProvider.configs()).thenReturn(configs);
    when(core.configurationProvider()).thenReturn(configProvider);

    AsyncScope scope = new AsyncScope("scope", "bucket", core, mock(ClusterEnvironment.class));

    AsyncCollection collection1 = scope.defaultCollection();
    AsyncCollection collection2 = scope.defaultCollection();

    AsyncCollection collection3 = scope.collection("foo");
    AsyncCollection collection4 = scope.collection("foo");

    assertEquals(collection1, collection2);
    assertEquals(collection3, collection4);
    assertNotEquals(collection1, collection3);
    assertNotEquals(collection2, collection4);
  }


}
