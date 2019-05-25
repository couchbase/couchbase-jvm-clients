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

package com.couchbase.client.core.service.strategy;

import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.endpoint.EndpointState;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.kv.GetRequest;
import com.couchbase.client.core.service.EndpointSelectionStrategy;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies the functionality of the {@link PartitionSelectionStrategy}.
 */
class PartitionSelectionStrategyTest {

  @Test
  void selectPinnedForBinaryWithKey() {
    EndpointSelectionStrategy strategy = new PartitionSelectionStrategy();

    Endpoint endpoint1 = mock(Endpoint.class);
    Endpoint endpoint2 = mock(Endpoint.class);
    Endpoint endpoint3 = mock(Endpoint.class);

    when(endpoint1.state()).thenReturn(EndpointState.CONNECTED);
    when(endpoint2.state()).thenReturn(EndpointState.CONNECTED);
    when(endpoint3.state()).thenReturn(EndpointState.CONNECTED);
    when(endpoint1.free()).thenReturn(true);
    when(endpoint2.free()).thenReturn(true);
    when(endpoint3.free()).thenReturn(true);

    List<Endpoint> endpoints = Arrays.asList(endpoint1, endpoint2, endpoint3);

    GetRequest request = mock(GetRequest.class);
    when(request.partition()).thenReturn((short) 12);
    Endpoint selected = strategy.select(request, endpoints);

    for (int i = 0; i < 1000; i++) {
      assertNotNull(selected);
      assertEquals(selected, endpoint1);
    }
  }

  @Test
  void selectNullIfPinedIsNotConnected() {
    EndpointSelectionStrategy strategy = new PartitionSelectionStrategy();

    Endpoint endpoint1 = mock(Endpoint.class);
    Endpoint endpoint2 = mock(Endpoint.class);
    Endpoint endpoint3 = mock(Endpoint.class);

    when(endpoint1.state()).thenReturn(EndpointState.DISCONNECTED);
    when(endpoint2.state()).thenReturn(EndpointState.CONNECTED);
    when(endpoint3.state()).thenReturn(EndpointState.CONNECTED);
    when(endpoint1.free()).thenReturn(true);
    when(endpoint2.free()).thenReturn(true);
    when(endpoint3.free()).thenReturn(true);

    List<Endpoint> endpoints = Arrays.asList(endpoint1, endpoint2, endpoint3);

    GetRequest request = mock(GetRequest.class);
    when(request.partition()).thenReturn((short) 12);
    Endpoint selected = strategy.select(request, endpoints);

    for (int i = 0; i < 1000; i++) {
      assertNull(selected);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void returnNullIfEmptyEndpointList() {
    EndpointSelectionStrategy strategy = new PartitionSelectionStrategy();

    Endpoint selected = strategy.select(mock(Request.class), Collections.emptyList());
    assertNull(selected);
  }

}