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

package com.couchbase.client.core.node;

import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.msg.view.ViewRequest;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ViewLocatorTest {

  @Test
  void dispatchesOnlyToHostsWithPrimaryPartitionsEnabled() {
    ViewLocator locator = new ViewLocator();

    ViewRequest request = mock(ViewRequest.class);
    when(request.bucket()).thenReturn("bucket");
    CouchbaseBucketConfig bucketConfig = mock(CouchbaseBucketConfig.class);
    ClusterConfig config = mock(ClusterConfig.class);
    when(config.bucketConfig("bucket")).thenReturn(bucketConfig);
    when(bucketConfig.hasPrimaryPartitionsOnNode(new NodeIdentifier("1.2.3.4", 1234))).thenReturn(true);
    when(bucketConfig.hasPrimaryPartitionsOnNode(new NodeIdentifier("1.2.3.5", 1234))).thenReturn(false);

    Node node1 = mock(Node.class);
    when(node1.identifier()).thenReturn(new NodeIdentifier("1.2.3.4", 1234));
    assertTrue(locator.nodeCanBeUsed(node1, request, config));

    Node node2 = mock(Node.class);
    when(node2.identifier()).thenReturn(new NodeIdentifier("1.2.3.5", 1234));
    assertFalse(locator.nodeCanBeUsed(node2, request, config));
  }

}
