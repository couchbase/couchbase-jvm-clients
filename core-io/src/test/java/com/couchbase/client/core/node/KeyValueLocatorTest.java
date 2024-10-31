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

import com.couchbase.client.core.Core;
import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.config.NodeInfo;
import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.msg.CancellationReason;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.RequestContext;
import com.couchbase.client.core.msg.kv.CarrierBucketConfigRequest;
import com.couchbase.client.core.msg.kv.GetRequest;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.couchbase.client.core.util.MockUtil.mockCore;
import static com.couchbase.client.core.topology.TopologyTestUtils.nodeId;
import static com.couchbase.client.core.topology.TopologyTestUtils.nodeInfo;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies the functionality of the {@link KeyValueLocator}.
 */
class KeyValueLocatorTest {

  @Test
  @SuppressWarnings("unchecked")
  void locateGetRequestForCouchbaseBucket() {
    Locator locator = new KeyValueLocator();

    NodeInfo nodeInfo1 = nodeInfo("192.168.56.101", emptyMap());
    NodeInfo nodeInfo2 = nodeInfo("192.168.56.102", emptyMap());

    GetRequest getRequestMock = mock(GetRequest.class);
    ClusterConfig configMock = mock(ClusterConfig.class);
    Node node1Mock = mock(Node.class);
    when(node1Mock.identifier()).thenReturn(nodeInfo1.id());
    Node node2Mock = mock(Node.class);
    when(node2Mock.identifier()).thenReturn(nodeInfo2.id());
    List<Node> nodes = new ArrayList<>(Arrays.asList(node1Mock, node2Mock));
    CouchbaseBucketConfig bucketMock = mock(CouchbaseBucketConfig.class);
    when(getRequestMock.bucket()).thenReturn("bucket");
    when(getRequestMock.key()).thenReturn("key".getBytes(UTF_8));
    CoreContext coreContext = new CoreContext(mockCore(), 1, mock(CoreEnvironment.class), mock(Authenticator.class));
    when(getRequestMock.context()).thenReturn(new RequestContext(coreContext, getRequestMock));
    when(configMock.bucketConfig("bucket")).thenReturn(bucketMock);
    when(bucketMock.nodes()).thenReturn(Arrays.asList(nodeInfo1, nodeInfo2));
    when(bucketMock.numberOfPartitions()).thenReturn(1024);
    when(bucketMock.nodeIndexForActive(656, false)).thenReturn((short) 0);
    when(bucketMock.nodeAtIndex(0)).thenReturn(nodeInfo1);

    locator.dispatch(getRequestMock, nodes, configMock, null);
    verify(node1Mock, times(1)).send(getRequestMock);
    verify(node2Mock, never()).send(getRequestMock);
  }

  @Test
  @SuppressWarnings("unchecked")
  void pickFastForwardIfAvailableAndNmvbSeen() {
    Locator locator = new KeyValueLocator();

    NodeInfo nodeInfo1 = nodeInfo("192.168.56.101", emptyMap());
    NodeInfo nodeInfo2 = nodeInfo("192.168.56.102", emptyMap());

    Node node1Mock = mock(Node.class);
    when(node1Mock.identifier()).thenReturn(nodeInfo1.id());
    Node node2Mock = mock(Node.class);
    when(node2Mock.identifier()).thenReturn(nodeInfo2.id());
    List<Node> nodes = new ArrayList<>(Arrays.asList(node1Mock, node2Mock));

    // Configure Cluster and Bucket config
    ClusterConfig configMock = mock(ClusterConfig.class);
    CouchbaseBucketConfig bucketMock = mock(CouchbaseBucketConfig.class);
    when(configMock.bucketConfig("bucket")).thenReturn(bucketMock);
    when(bucketMock.nodes()).thenReturn(Arrays.asList(nodeInfo1, nodeInfo2));
    when(bucketMock.numberOfPartitions()).thenReturn(1024);
    when(bucketMock.nodeAtIndex(0)).thenReturn(nodeInfo1);
    when(bucketMock.nodeAtIndex(1)).thenReturn(nodeInfo2);
    when(bucketMock.hasFastForwardMap()).thenReturn(true);

    // Fake a vbucket move in ffwd map from node 0 to node 1
    when(bucketMock.nodeIndexForActive(656, false)).thenReturn((short) 0);
    when(bucketMock.nodeIndexForActive(656, true)).thenReturn((short) 1);

    // Create Request
    GetRequest getRequest = mock(GetRequest.class);
    when(getRequest.bucket()).thenReturn("bucket");
    when(getRequest.key()).thenReturn("key".getBytes(UTF_8));
    RequestContext requestCtx = mock(RequestContext.class);
    when(getRequest.context()).thenReturn(requestCtx);

    // Dispatch with retry 0
    when(requestCtx.retryAttempts()).thenReturn(0);
    locator.dispatch(getRequest, nodes, configMock, null);
    verify(node1Mock, times(1)).send(getRequest);
    verify(node2Mock, never()).send(getRequest);

    // Dispatch with retry 1 but no nmvb seen, still go to the active
    when(requestCtx.retryAttempts()).thenReturn(1);
    locator.dispatch(getRequest, nodes, configMock, null);
    verify(node1Mock, times(2)).send(getRequest);
    verify(node2Mock, never()).send(getRequest);

    // Dispatch with retry 2, now we see a NMVB
    when(requestCtx.retryAttempts()).thenReturn(2);
    when(getRequest.rejectedWithNotMyVbucket()).thenReturn(1);
    locator.dispatch(getRequest, nodes, configMock, null);
    verify(node1Mock, times(2)).send(getRequest);
    verify(node2Mock, times(1)).send(getRequest);
  }

  @Test
  @SuppressWarnings("unchecked")
  void pickCurrentIfNoFFMapAndRetry() {
    Locator locator = new KeyValueLocator();

    NodeInfo nodeInfo1 = nodeInfo("192.168.56.101", emptyMap());
    NodeInfo nodeInfo2 = nodeInfo("192.168.56.102", emptyMap());

    Node node1Mock = mock(Node.class);
    when(node1Mock.identifier()).thenReturn(nodeInfo1.id());
    Node node2Mock = mock(Node.class);
    when(node2Mock.identifier()).thenReturn(nodeInfo2.id());
    List<Node> nodes = new ArrayList<>(Arrays.asList(node1Mock, node2Mock));

    // Configure Cluster and Bucket config
    ClusterConfig configMock = mock(ClusterConfig.class);
    CouchbaseBucketConfig bucketMock = mock(CouchbaseBucketConfig.class);
    when(configMock.bucketConfig("bucket")).thenReturn(bucketMock);
    when(bucketMock.nodes()).thenReturn(Arrays.asList(nodeInfo1, nodeInfo2));
    when(bucketMock.numberOfPartitions()).thenReturn(1024);
    when(bucketMock.nodeAtIndex(0)).thenReturn(nodeInfo1);
    when(bucketMock.nodeAtIndex(1)).thenReturn(nodeInfo2);
    when(bucketMock.hasFastForwardMap()).thenReturn(false);

    // Fake a vbucket move in ffwd map from node 0 to node 1
    when(bucketMock.nodeIndexForActive(656, false)).thenReturn((short) 0);
    when(bucketMock.nodeIndexForActive(656, true)).thenReturn((short) 1);

    // Create Request
    GetRequest getRequest = mock(GetRequest.class);
    when(getRequest.bucket()).thenReturn("bucket");
    when(getRequest.key()).thenReturn("key".getBytes(UTF_8));
    RequestContext requestCtx = mock(RequestContext.class);
    when(getRequest.context()).thenReturn(requestCtx);

    // Dispatch with retry 0
    when(requestCtx.retryAttempts()).thenReturn(0);
    locator.dispatch(getRequest, nodes, configMock, null);
    verify(node1Mock, times(1)).send(getRequest);
    verify(node2Mock, never()).send(getRequest);

    // Dispatch with retry 1
    when(requestCtx.retryAttempts()).thenReturn(1);
    locator.dispatch(getRequest, nodes, configMock, null);
    verify(node1Mock, times(2)).send(getRequest);
    verify(node2Mock, never()).send(getRequest);

    // Dispatch with retry 5
    when(requestCtx.retryAttempts()).thenReturn(5);
    locator.dispatch(getRequest, nodes, configMock, null);
    verify(node1Mock, times(3)).send(getRequest);
    verify(node2Mock, never()).send(getRequest);
  }

  @Test
  @SuppressWarnings("unchecked")
  void pickCurrentIfNoFFMapAndNmvbSeen() {
    Locator locator = new KeyValueLocator();

    NodeInfo nodeInfo1 = nodeInfo("192.168.56.101", emptyMap());
    NodeInfo nodeInfo2 = nodeInfo("192.168.56.102", emptyMap());

    Node node1Mock = mock(Node.class);
    when(node1Mock.identifier()).thenReturn(nodeInfo1.id());
    Node node2Mock = mock(Node.class);
    when(node2Mock.identifier()).thenReturn(nodeInfo2.id());
    List<Node> nodes = new ArrayList<>(Arrays.asList(node1Mock, node2Mock));

    // Configure Cluster and Bucket config
    ClusterConfig configMock = mock(ClusterConfig.class);
    CouchbaseBucketConfig bucketMock = mock(CouchbaseBucketConfig.class);
    when(configMock.bucketConfig("bucket")).thenReturn(bucketMock);
    when(bucketMock.nodes()).thenReturn(Arrays.asList(nodeInfo1, nodeInfo2));
    when(bucketMock.numberOfPartitions()).thenReturn(1024);
    when(bucketMock.nodeAtIndex(0)).thenReturn(nodeInfo1);
    when(bucketMock.nodeAtIndex(1)).thenReturn(nodeInfo2);
    when(bucketMock.hasFastForwardMap()).thenReturn(false);

    // Fake a vbucket move in ffwd map from node 0 to node 1
    when(bucketMock.nodeIndexForActive(656, false)).thenReturn((short) 0);
    when(bucketMock.nodeIndexForActive(656, true)).thenReturn((short) 1);

    // Create Request
    GetRequest getRequest = mock(GetRequest.class);
    when(getRequest.bucket()).thenReturn("bucket");
    when(getRequest.key()).thenReturn("key".getBytes(UTF_8));
    RequestContext requestCtx = mock(RequestContext.class);
    when(getRequest.context()).thenReturn(requestCtx);

    when(getRequest.rejectedWithNotMyVbucket()).thenReturn(9);
    locator.dispatch(getRequest, nodes, configMock, null);
    verify(node1Mock, times(1)).send(getRequest);
    verify(node2Mock, never()).send(getRequest);

    when(getRequest.rejectedWithNotMyVbucket()).thenReturn(1);
    locator.dispatch(getRequest, nodes, configMock, null);
    verify(node1Mock, times(2)).send(getRequest);
    verify(node2Mock, never()).send(getRequest);
  }

  @Test
  void cancelsTargetedRequestIfNodeListEmpty() {
    Locator locator = new KeyValueLocator();

    Request<?> request = mock(CarrierBucketConfigRequest.class);
    when(request.target()).thenReturn(nodeId("localhost", 8091));

    locator.dispatch(request, Collections.emptyList(), null, null);

    verify(request, times(1)).cancel(CancellationReason.TARGET_NODE_REMOVED);
  }

  @Test
  void cancelsTargetedRequestIfNodeNotInList() {
    Locator locator = new KeyValueLocator();

    Request<?> request = mock(CarrierBucketConfigRequest.class);
    when(request.target()).thenReturn(nodeId("hostb", 8091));

    Node node = mock(Node.class);
    when(node.state()).thenReturn(NodeState.CONNECTED);
    when(node.identifier()).thenReturn(nodeId("hosta", 8091));

    locator.dispatch(request, Collections.singletonList(node), null, null);

    verify(request, times(1)).cancel(CancellationReason.TARGET_NODE_REMOVED);
  }

}
