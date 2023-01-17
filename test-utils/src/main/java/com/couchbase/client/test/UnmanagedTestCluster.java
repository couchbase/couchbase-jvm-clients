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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import okhttp3.Credentials;
import okhttp3.FormBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import javax.naming.NamingException;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.couchbase.client.test.DnsSrvUtil.fromDnsSrv;
import static java.nio.charset.StandardCharsets.UTF_8;

public class UnmanagedTestCluster extends TestCluster {
  private static Logger logger = LoggerFactory.getLogger(UnmanagedTestCluster.class);

  private static final int DEFAULT_PROTOSTELLAR_TLS_PORT = 18098;

  private final OkHttpClient httpClient;
  private final String seedHost;
  private final boolean isDnsSrv;
  private final String adminUsername;
  private final String adminPassword;
  private volatile String bucketname;
  private final int numReplicas;
  private final String certsFile;
  private volatile boolean runWithTLS;
  private final String baseUrl;
  private final boolean isProtostellar;

  UnmanagedTestCluster(final Properties properties) {
    // localhost:8091 or couchbases://localhost:8091 or protostellar://localhost:8091 or protostellar://localhost
    String[] split = properties.getProperty("cluster.unmanaged.seed").split(":");
    isProtostellar = split[0].equals("protostellar");
    seedHost = split[split.length - 2].replace("//", "");
    int seedPort = 0;
    try {
      seedPort = Integer.parseInt(split[split.length - 1]);
    }
    catch (NumberFormatException err) {
      // Handling couchbase://localhost et al.
    }
    adminUsername = properties.getProperty("cluster.adminUsername");
    adminPassword = properties.getProperty("cluster.adminPassword");
    numReplicas = Integer.parseInt(properties.getProperty("cluster.unmanaged.numReplicas"));
    certsFile = properties.getProperty("cluster.unmanaged.certsFile");
    runWithTLS = Boolean.parseBoolean(properties.getProperty("cluster.unmanaged.runWithTLS")) || seedPort == 18091;
    isDnsSrv = Boolean.parseBoolean(properties.getProperty("cluster.unmanaged.dnsSrv"));
    bucketname = Optional.ofNullable(properties.getProperty("cluster.unmanaged.bucket")).orElse("");
    httpClient = setupHttpClient(runWithTLS);
    baseUrl = (runWithTLS ? "https://" : "http://") + getNodeUrl(isDnsSrv, seedHost, runWithTLS) + (seedPort == 0 ? "" : (":" + seedPort));
  }

  @Override
  ClusterType type() {
    //Assuming running against Capella when provided with a DNS SRV hostname, and a pre-created bucket
    return isDnsSrv && !bucketname.isEmpty() ? ClusterType.CAPELLA : ClusterType.UNMANAGED;
  }

  @Override
  TestClusterConfig _start() throws Exception {
    if (bucketname.isEmpty()) {
      bucketname = UUID.randomUUID().toString();

      Response postResponse = httpClient.newCall(new Request.Builder()
        .header("Authorization", Credentials.basic(adminUsername, adminPassword))
        .url(baseUrl + "/pools/default/buckets")
        .post(new FormBody.Builder()
          .add("name", bucketname)
          .add("bucketType", "membase")
          .add("ramQuotaMB", "100")
          .add("replicaNumber", Integer.toString(numReplicas))
          .add("flushEnabled", "1")
          .build())
        .build())
        .execute();

      if (postResponse.code() != 202) {
        throw new Exception("Could not create bucket: "
          + postResponse + ", Reason: "
          + postResponse.body().string());
      }
    }

    Response getResponse = httpClient.newCall(new Request.Builder()
      .header("Authorization", Credentials.basic(adminUsername, adminPassword))
      .url(baseUrl + "/pools/default/b/" + bucketname)
      .build())
      .execute();

    String raw = getResponse.body().string();

    logger.info("Bucket raw results: {}", raw);

    waitUntilAllNodesHealthy();

    Response getClusterVersionResponse = httpClient.newCall(new Request.Builder()
      .header("Authorization", Credentials.basic(adminUsername, adminPassword))
      .url(baseUrl + "/pools")
      .build())
      .execute();

    ClusterVersion clusterVersion = parseClusterVersion(getClusterVersionResponse);

    Optional<List<X509Certificate>> certs = loadClusterCertificate();

    if (certsFile != null) {
      certs = loadMultipleRootCertsFromFile();
      runWithTLS = true;
    }

    List<TestNodeConfig> nodeConfigs;
    if (isDnsSrv) {
      // Use DNS SRV connection string in tests
      nodeConfigs = new ArrayList<>();
      nodeConfigs.add(new TestNodeConfig(seedHost, null, true, Optional.empty()));
    } else if (isProtostellar) {
      nodeConfigs = new ArrayList<>();
      nodeConfigs.add(new TestNodeConfig(seedHost, null, false, Optional.of(DEFAULT_PROTOSTELLAR_TLS_PORT)));
    } else {
      nodeConfigs = nodesFromRaw(seedHost, raw);
    }

    return new TestClusterConfig(
      bucketname,
      adminUsername,
      adminPassword,
      nodeConfigs,
      replicasFromRaw(raw),
      certs,
      capabilitiesFromRaw(raw, clusterVersion),
      clusterVersion,
      runWithTLS
    );
  }

  private Optional<List<X509Certificate>> loadClusterCertificate() {
    try {
      Response getResponse = httpClient.newCall(new Request.Builder()
        .header("Authorization", Credentials.basic(adminUsername, adminPassword))
        .url(baseUrl + "/pools/default/certificate")
        .build())
        .execute();

      String raw = getResponse.body().string();

      CertificateFactory cf = CertificateFactory.getInstance("X.509");
      Certificate cert = cf.generateCertificate(new ByteArrayInputStream(raw.getBytes(UTF_8)));
      return Optional.of(Collections.singletonList((X509Certificate) cert));
    } catch (Exception ex) {
      // could not load certificate, maybe add logging? could be CE instance.
      return Optional.empty();
    }
  }

  private Optional<List<X509Certificate>> loadMultipleRootCertsFromFile() {
    if (certsFile != null) {
      try (FileInputStream fis = new FileInputStream(certsFile)){
        List<X509Certificate> certs = new ArrayList<>();
        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        Collection certCollection = cf.generateCertificates(fis);
        certCollection.forEach(c -> certs.add((X509Certificate) c));
        return Optional.of(certs);
      } catch (Exception ex) {
        // Could not load certs
        return Optional.empty();
      }
    }
    return Optional.empty();
  }

  private void waitUntilAllNodesHealthy() throws Exception {
    while(true) {
      Response getResponse = httpClient.newCall(new Request.Builder()
        .header("Authorization", Credentials.basic(adminUsername, adminPassword))
        .url(baseUrl + "/pools/default/")
        .build())
        .execute();

      String raw = getResponse.body().string();

      Map<String, Object> decoded;
      try {
        decoded = (Map<String, Object>)
          MAPPER.readValue(raw, Map.class);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      List<Map<String, Object>> nodes = (List<Map<String, Object>>) decoded.get("nodes");
      int healthy = 0;
      for (Map<String, Object> node : nodes) {
        String status = (String) node.get("status");
        if (status.equals("healthy")) {
          healthy++;
        }
      }
      if (healthy == nodes.size()) {
        break;
      }
      Thread.sleep(100);
    }
  }

  @Override
  public void close() {
    try {
      httpClient.newCall(new Request.Builder()
        .header("Authorization", Credentials.basic(adminUsername, adminPassword))
        .url(baseUrl + "/pools/default/buckets/" + bucketname)
        .delete()
        .build()).execute();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private OkHttpClient setupHttpClient(boolean useTLS) {
    OkHttpClient.Builder builder =  new OkHttpClient().newBuilder()
      .connectTimeout(30, TimeUnit.SECONDS)
      .readTimeout(30, TimeUnit.SECONDS)
      .writeTimeout(30, TimeUnit.SECONDS);

    //NB: Not secure - ok for testing purposes only
    TrustManager[] trustAllCerts = new TrustManager[]{
      new X509TrustManager() {
        @Override
        public void checkClientTrusted(java.security.cert.X509Certificate[] chain, String authType) {
        }

        @Override
        public void checkServerTrusted(java.security.cert.X509Certificate[] chain, String authType) {
        }

        @Override
        public java.security.cert.X509Certificate[] getAcceptedIssuers() {
          return new java.security.cert.X509Certificate[]{};
        }
      }
    };

    if (useTLS) {
      try {
        SSLContext sslContext = SSLContext.getInstance("SSL");
        sslContext.init(null, trustAllCerts, new java.security.SecureRandom());
        builder
          .sslSocketFactory(sslContext.getSocketFactory(), (X509TrustManager) trustAllCerts[0])
          .hostnameVerifier((hostname, session) -> true);
      } catch (NoSuchAlgorithmException | KeyManagementException e) {
        logger.warn("Couldn't create secure http/s client, using basic http", e);
      }
    }
    return builder.build();
  }

  private String getNodeUrl(boolean isDnsSrv, String seedHost, boolean runWithTLS) {
    if (isDnsSrv) {
      try {
        return fromDnsSrv(seedHost, false, runWithTLS, null).get(0);
      } catch (NamingException e) {
        logger.warn("Failed to resolve DNS SRV records, attempting to run with seed host", e);
      }
    }
    return seedHost;
  }

  @Override
  public boolean isProtostellar() {
    return isProtostellar;
  }
}
