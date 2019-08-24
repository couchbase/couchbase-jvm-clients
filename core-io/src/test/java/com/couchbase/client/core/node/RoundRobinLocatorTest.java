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

package com.couchbase.client.core.node;

import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.msg.query.QueryRequest;
import com.couchbase.client.core.service.ServiceType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies the functionality of the {@link RoundRobinLocator}.
 */
class RoundRobinLocatorTest {

  @Test
  void selectNextNode() {
    Locator locator = new RoundRobinLocator(ServiceType.QUERY, 0);

    QueryRequest request = mock(QueryRequest.class);
    ClusterConfig configMock = mock(ClusterConfig.class);
    when(configMock.hasClusterOrBucketConfig()).thenReturn(true);

    Node node1Mock = mock(Node.class);
    when(node1Mock.serviceEnabled(ServiceType.QUERY)).thenReturn(true);
    when(node1Mock.identifier()).thenReturn(new NodeIdentifier("192.168.56.101", 8091));
    Node node2Mock = mock(Node.class);
    when(node2Mock.serviceEnabled(ServiceType.QUERY)).thenReturn(true);
    when(node2Mock.identifier()).thenReturn(new NodeIdentifier("192.168.56.102", 8091));
    List<Node> nodes = new ArrayList<>(Arrays.asList(node1Mock, node2Mock));

    locator.dispatch(request, nodes, configMock, null);
    verify(node1Mock, times(1)).send(request);
    verify(node2Mock, never()).send(request);

    locator.dispatch(request, nodes, configMock, null);
    verify(node1Mock, times(1)).send(request);
    verify(node2Mock, times(1)).send(request);

    locator.dispatch(request, nodes, configMock, null);
    verify(node1Mock, times(2)).send(request);
    verify(node2Mock, times(1)).send(request);
  }

  @Test
  void skipNodeWithoutServiceEnabled() {
    Locator locator = new RoundRobinLocator(ServiceType.QUERY, 0);

    QueryRequest request = mock(QueryRequest.class);
    ClusterConfig configMock = mock(ClusterConfig.class);
    when(configMock.hasClusterOrBucketConfig()).thenReturn(true);

    Node node1Mock = mock(Node.class);
    when(node1Mock.identifier()).thenReturn(new NodeIdentifier("192.168.56.101", 8091));
    when(node1Mock.serviceEnabled(ServiceType.QUERY)).thenReturn(false);
    Node node2Mock = mock(Node.class);
    when(node2Mock.identifier()).thenReturn(new NodeIdentifier("192.168.56.102", 8091));
    when(node2Mock.serviceEnabled(ServiceType.QUERY)).thenReturn(true);
    Node node3Mock = mock(Node.class);
    when(node3Mock.identifier()).thenReturn(new NodeIdentifier("192.168.56.103", 8091));
    when(node3Mock.serviceEnabled(ServiceType.QUERY)).thenReturn(false);
    List<Node> nodes = new ArrayList<>(Arrays.asList(node1Mock, node2Mock, node3Mock));

    locator.dispatch(request, nodes, configMock, null);
    verify(node1Mock, never()).send(request);
    verify(node2Mock, times(1)).send(request);
    verify(node3Mock, never()).send(request);

    locator.dispatch(request, nodes, configMock, null);
    verify(node1Mock, never()).send(request);
    verify(node2Mock, times(2)).send(request);
    verify(node3Mock, never()).send(request);

    locator.dispatch(request, nodes, configMock, null);
    verify(node1Mock, never()).send(request);
    verify(node2Mock, times(3)).send(request);
    verify(node3Mock, never()).send(request);

    locator.dispatch(request, nodes, configMock, null);
    verify(node1Mock, never()).send(request);
    verify(node2Mock, times(4)).send(request);
    verify(node3Mock, never()).send(request);
  }

  @Test
  void shouldDistributeFairlyUnderMDS() {
    Locator locator = new RoundRobinLocator(ServiceType.QUERY, 0);

    QueryRequest request = mock(QueryRequest.class);
    ClusterConfig configMock = mock(ClusterConfig.class);
    when(configMock.hasClusterOrBucketConfig()).thenReturn(true);

    Node node1Mock = mock(Node.class);
    when(node1Mock.identifier()).thenReturn(new NodeIdentifier("192.168.56.101", 8091));
    when(node1Mock.serviceEnabled(ServiceType.QUERY)).thenReturn(false);
    Node node2Mock = mock(Node.class);
    when(node2Mock.identifier()).thenReturn(new NodeIdentifier("192.168.56.102", 8091));
    when(node2Mock.serviceEnabled(ServiceType.QUERY)).thenReturn(false);
    Node node3Mock = mock(Node.class);
    when(node3Mock.identifier()).thenReturn(new NodeIdentifier("192.168.56.103", 8091));
    when(node3Mock.serviceEnabled(ServiceType.QUERY)).thenReturn(true);
    Node node4Mock = mock(Node.class);
    when(node4Mock.identifier()).thenReturn(new NodeIdentifier("192.168.56.104", 8091));
    when(node4Mock.serviceEnabled(ServiceType.QUERY)).thenReturn(true);
    List<Node> nodes = new ArrayList<>(Arrays.asList(node1Mock, node2Mock, node3Mock, node4Mock));

    locator.dispatch(request, nodes, configMock, null);
    verify(node1Mock, never()).send(request);
    verify(node2Mock, never()).send(request);
    verify(node3Mock, times(1)).send(request);
    verify(node4Mock, never()).send(request);

    locator.dispatch(request, nodes, configMock, null);
    verify(node1Mock, never()).send(request);
    verify(node2Mock, never()).send(request);
    verify(node3Mock, times(1)).send(request);
    verify(node4Mock, times(1)).send(request);

    locator.dispatch(request, nodes, configMock, null);
    verify(node1Mock, never()).send(request);
    verify(node2Mock, never()).send(request);
    verify(node3Mock, times(2)).send(request);
    verify(node4Mock, times(1)).send(request);

    locator.dispatch(request, nodes, configMock, null);
    verify(node1Mock, never()).send(request);
    verify(node2Mock, never()).send(request);
    verify(node3Mock, times(2)).send(request);
    verify(node4Mock, times(2)).send(request);
  }

}