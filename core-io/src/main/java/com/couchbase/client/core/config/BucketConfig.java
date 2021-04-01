/*
 * Copyright (c) 2016 Couchbase, Inc.
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

package com.couchbase.client.core.config;

import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonSubTypes;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Represents a Couchbase Bucket Configuration.
 */
@JsonTypeInfo(
  use = JsonTypeInfo.Id.NAME,
  property = "nodeLocator")
@JsonSubTypes({
  @JsonSubTypes.Type(value = CouchbaseBucketConfig.class, name = "vbucket"),
  @JsonSubTypes.Type(value = MemcachedBucketConfig.class, name = "ketama")})
public interface BucketConfig {

  /**
   * Returns the UUID of the bucket, or {@code null} if the bucket does not have a UUID.
   *
   * <p>The UUID is an opaque value assigned when the bucket is created.
   * If the bucket is deleted and a new bucket is created with the same name,
   * the new bucket will have a different UUID.</p>
   *
   * @return bucket UUID, or {@code null}.
   */
  String uuid();

  /**
   * The name of the bucket.
   *
   * @return name of the bucket.
   */
  String name();

  /**
   * The type of node locator in use for this bucket.
   *
   * @return the node locator type.
   */
  BucketNodeLocator locator();

  /**
   * The HTTP Uri for this bucket configuration.
   *
   * @return the uri.
   */
  String uri();

  /**
   * The HTTP Streaming URI for this bucket.
   *
   * @return the streaming uri.
   */
  String streamingUri();

  /**
   * The list of nodes associated with this bucket.
   *
   * @return the list of nodes.
   */
  List<NodeInfo> nodes();

  /**
   * Returns true if the config indicates the cluster is undergoing
   * a transition (such as a rebalance operation).
   *
   * @return true if a transition is in progress.
   */
  boolean tainted();

  /**
   * Revision number (optional) for that configuration.
   *
   * @return the rev number, might be 0.
   */
  long rev();

  /**
   * The bucket type.
   *
   * @return the bucket type.
   */
  BucketType type();

  /**
   * Check if the service is enabled on the bucket.
   *
   * @param type the type to check.
   * @return true if it is, false otherwise.
   */
  boolean serviceEnabled(ServiceType type);

  /**
   * Returns true if the config has a fast forward map that describes what the
   * topology of the cluster will be after the current rebalance operation completes.
   *
   * @return true if it does, false otherwise.
   */
  boolean hasFastForwardMap();

  /**
   * Returns the cluster capabilities reported by the server.
   */
  Map<ServiceType, Set<ClusterCapabilities>> clusterCapabilities();

  /**
   * Returns all the capabilities that are enabled and recognized on this bucket.
   */
  Set<BucketCapabilities> bucketCapabilities();

  /**
   * Returns the port information from the "nodesExt" section.
   * <p>
   * NOTE: If you are unsure if you want to use this api, very likely you want to use {@link #nodes()} instead!
   * <p>
   * The nodes API is very similar to this port infos (in fact, they are built from each other), but there is one
   * big difference: when the KV and VIEW service are defined on a bucket but not yet ready, the nodes API will
   * NOT include them in the services list, while this API will. Most of the time you do not want to consume a
   * service until it is ready, but for some functionality (like waitUntilReady) it can be needed to "look ahead"
   * what will become active and wait for it.
   *
   * @return the port infos.
   */
  List<PortInfo> portInfos();

}