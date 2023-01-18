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

package com.couchbase.client.test;

// CHECKSTYLE:OFF IllegalImport - Allow unbundled Jackson

import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import okhttp3.FormBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static java.net.HttpURLConnection.HTTP_OK;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Uses containers to manage the test cluster (by using testcontainers).
 *
 * @since 2.0.0
 */
public class ContainerizedTestCluster extends TestCluster {
  private static final Logger LOGGER = LoggerFactory.getLogger(ContainerizedTestCluster.class);

  private final SupportedVersion version;
  private final int numNodes;
  private final List<CouchbaseContainer> containers = new CopyOnWriteArrayList<>();

  ContainerizedTestCluster(final Properties properties) {
    version = SupportedVersion.fromString(properties.getProperty("cluster.containerized.version"));
    numNodes = Integer.parseInt(properties.getProperty("cluster.containerized.numNodes"));
    adminUsername = properties.getProperty("cluster.adminUsername");
    adminPassword = properties.getProperty("cluster.adminPassword");
    this.baseUrl = "http://%s:%s";
    httpClient = new OkHttpClient.Builder().build();
  }

  @Override
  ClusterType type() {
    return ClusterType.CONTAINERIZED;
  }

  @Override
  TestClusterConfig _start() throws Exception {
    LOGGER.info("Starting Containerized Cluster of {} nodes", numNodes);
    long start = System.nanoTime();
    IntStream.range(0, numNodes)
      .mapToObj(x -> new CouchbaseContainer(version))
      .peek(GenericContainer::start)
      .forEach(containers::add);
    long end = System.nanoTime();
    LOGGER.debug("Starting Containerized Cluster took {}s", TimeUnit.NANOSECONDS.toSeconds(end - start));

    CouchbaseContainer firstContainer = containers.get(0);
    String seedHost = firstContainer.getContainerIpAddress();
    int seedPort = firstContainer.getMappedPort(CouchbaseContainer.CouchbasePort.REST);
    this.baseUrl = String.format(baseUrl, seedHost, seedPort);
    initFirstNode(firstContainer, seedPort);
    // todo: then add all others with services to the cluster
    // todo: then rebalance

    bucketname = UUID.randomUUID().toString();
    Request.Builder builder = builderWithAuth();
    Response postResponse = httpClient.newCall(builder
        .url(baseUrl + BUCKET_URL)
        .post(new FormBody.Builder()
          .add("name", bucketname)
          .add("ramQuotaMB", "100")
          .build())
        .build())
      .execute();

    if (postResponse.code() != ACCEPTED) {
      throw new Exception("Could not create bucket: " + postResponse);
    }

    String raw = getRawConfig(builder);
    ClusterVersion clusterVersion = getClusterVersionFromServer(builder);

    return new TestClusterConfig(
      bucketname,
      adminUsername,
      adminPassword,
      nodesFromRaw(seedHost, raw),
      replicasFromRaw(raw),
      loadClusterCertificate(),
      capabilitiesFromRaw(raw, clusterVersion),
      clusterVersion,
      false
    );
  }

  private void initFirstNode(CouchbaseContainer container, int seedPort) throws Exception {
    Request.Builder builder = builderWithAuth();
    httpClient.newCall(builder
      .url(baseUrl + POOLS_URL)
      .post(new FormBody.Builder()
        .add("memoryQuota", "256")
        .add("indexMemoryQuota", "256")
        .build())
      .build())
      .execute();

    httpClient.newCall(builder
      .url(baseUrl + "/node/controller/setupServices")
      .post(new FormBody.Builder()
        .add("services", "kv,n1ql,index,fts")
        .build())
      .build())
      .execute();

    httpClient.newCall(builder
        .url(baseUrl + "/settings/web")
      .post(new FormBody.Builder()
        .add("username", adminUsername)
        .add("password", adminPassword)
        .add("port", String.valueOf(seedPort))
        .build())
      .build())
      .execute();

    createNodeWaitStrategy().waitUntilReady(container);
  }

  private HttpWaitStrategy createNodeWaitStrategy() {
    return new HttpWaitStrategy()
      .forPath(POOLS_URL)
      .withBasicCredentials(adminUsername, adminPassword)
      .forStatusCode(HTTP_OK)
      .forResponsePredicate(response -> {
          try {
            return Optional.of(
              MAPPER.readTree(response.getBytes(UTF_8)))
                .map(n -> n.at("/nodes/0/status"))
                .map(JsonNode::asText)
                .map("healthy"::equals)
                .orElse(false);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      );
  }

  @Override
  public void close() {
    LOGGER.info("Stopping Containerized Cluster of {} nodes", containers.size());
    long start = System.nanoTime();
    containers.forEach(CouchbaseContainer::stop);

    long end = System.nanoTime();
    LOGGER.debug("Stopping Containerized Cluster took {}s", TimeUnit.NANOSECONDS.toSeconds(end - start));
  }
}
