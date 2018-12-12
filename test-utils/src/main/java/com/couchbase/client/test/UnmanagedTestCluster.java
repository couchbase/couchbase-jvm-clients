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

import org.testcontainers.shaded.okhttp3.Credentials;
import org.testcontainers.shaded.okhttp3.FormBody;
import org.testcontainers.shaded.okhttp3.OkHttpClient;
import org.testcontainers.shaded.okhttp3.Request;
import org.testcontainers.shaded.okhttp3.Response;

import java.util.Properties;
import java.util.UUID;

public class UnmanagedTestCluster extends TestCluster {

  private final OkHttpClient httpClient = new OkHttpClient.Builder().build();
  private final String seedHost;
  private final int seedPort;
  private final String adminUsername;
  private final String adminPassword;
  private volatile String bucketname;

  UnmanagedTestCluster(final Properties properties) {
    seedHost = properties.getProperty("cluster.unmanaged.seed").split(":")[0];
    seedPort = Integer.parseInt(properties.getProperty("cluster.unmanaged.seed").split(":")[1]);
    adminUsername = properties.getProperty("cluster.adminUsername");
    adminPassword = properties.getProperty("cluster.adminPassword");
  }

  @Override
  ClusterType type() {
    return ClusterType.UNMANAGED;
  }

  @Override
  TestClusterConfig _start() throws Exception {
    bucketname = UUID.randomUUID().toString();

    Response postResponse = httpClient.newCall(new Request.Builder()
      .header("Authorization", Credentials.basic(adminUsername, adminPassword))
      .url("http://" + seedHost + ":" + seedPort + "/pools/default/buckets")
      .post(new FormBody.Builder()
        .add("name", bucketname)
        .add("ramQuotaMB", "100")
        .build())
      .build())
      .execute();

    if (postResponse.code() != 202) {
      throw new Exception("Could not create bucket: " + postResponse);
    }

    Response getResponse = httpClient.newCall(new Request.Builder()
      .header("Authorization", Credentials.basic(adminUsername, adminPassword))
      .url("http://" + seedHost + ":" + seedPort + "/pools/default/b/" + bucketname)
      .build())
    .execute();

    return new TestClusterConfig(
      bucketname,
      adminUsername,
      adminPassword,
      nodesFromRaw(seedHost, getResponse.body().string())
    );
  }

  @Override
  public void close() {
    try {
      httpClient.newCall(new Request.Builder()
        .header("Authorization", Credentials.basic(adminUsername, adminPassword))
        .url("http://" + seedHost + ":" + seedPort + "/pools/default/buckets/"+bucketname)
        .delete()
        .build()).execute();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
}
