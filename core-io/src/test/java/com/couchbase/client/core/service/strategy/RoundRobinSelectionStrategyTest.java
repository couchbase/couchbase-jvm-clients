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

package com.couchbase.client.core.service.strategy;

import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.endpoint.EndpointState;
import com.couchbase.client.core.msg.Request;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

class RoundRobinSelectionStrategyTest {

  @Test
  @SuppressWarnings("unchecked")
  void testRoundRobinSelectOverIntegerMaxValue() {
    RoundRobinSelectionStrategy strategy = new RoundRobinSelectionStrategy();
    Endpoint a = Mockito.mock(Endpoint.class);
    Endpoint b = Mockito.mock(Endpoint.class);
    Endpoint c = Mockito.mock(Endpoint.class);
    Endpoint d = Mockito.mock(Endpoint.class);
    Endpoint e = Mockito.mock(Endpoint.class);
    when(a.state()).thenReturn(EndpointState.CONNECTED);
    when(a.freeToWrite()).thenReturn(true);
    when(b.state()).thenReturn(EndpointState.CONNECTED);
    when(b.freeToWrite()).thenReturn(true);
    when(c.state()).thenReturn(EndpointState.CONNECTED);
    when(c.freeToWrite()).thenReturn(true);
    when(d.state()).thenReturn(EndpointState.CONNECTED);
    when(d.freeToWrite()).thenReturn(true);
    when(e.state()).thenReturn(EndpointState.CONNECTED);
    when(e.freeToWrite()).thenReturn(true);
    List<Endpoint> endpoints = Arrays.asList(a, b, c, d, e);
    Request request = Mockito.mock(Request.class);

    strategy.setSkip(Integer.MAX_VALUE - 2);

    //selecting brings skip to max-value - 1
    strategy.select(request, endpoints);
    int skipStart = strategy.currentSkip();
    assertTrue(skipStart > 1000);

    //max-value
    strategy.select(request, endpoints);
    assertEquals(skipStart + 1, strategy.currentSkip());
    assertTrue(strategy.currentSkip() > 0);

    //max-value + 1: wrapping
    Endpoint selected = strategy.select(request, endpoints);
    assertEquals(0, strategy.currentSkip());
    assertEquals(selected, a);

    //following selects will select B, C, D, E, A and increment skip to 5
    selected = strategy.select(request, endpoints);
    assertEquals(1, strategy.currentSkip());
    assertEquals(selected, b);

    selected = strategy.select(request, endpoints);
    assertEquals(2, strategy.currentSkip());
    assertEquals(selected, c);

    selected = strategy.select(request, endpoints);
    assertEquals(3, strategy.currentSkip());
    assertEquals(selected, d);

    selected = strategy.select(request, endpoints);
    assertEquals(4, strategy.currentSkip());
    assertEquals(selected, e);

    selected = strategy.select(request, endpoints);
    assertEquals(5, strategy.currentSkip());
    assertEquals(selected, a);
  }

}