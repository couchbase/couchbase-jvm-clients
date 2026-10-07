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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.json.JsonMapper;
import okhttp3.Credentials;
import okhttp3.FormBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.NamingException;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
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
import java.util.stream.Collectors;

import static com.couchbase.client.test.DnsSrvUtil.fromDnsSrv;
import static com.couchbase.client.test.Util.urlEncode;
import static java.nio.charset.StandardCharsets.UTF_8;

public class UnmanagedTestCluster extends TestCluster {
  private static Logger logger = LoggerFactory.getLogger(UnmanagedTestCluster.class);

  private static final int DEFAULT_PROTOSTELLAR_TLS_PORT = 18098;

  private static final JsonMapper jsonMapper = JsonMapper.builder().build();

  private final OkHttpClient httpClient;
  private final String seedHost;
  private final boolean isDnsSrv;
  private final String adminUsername;
  private final String adminPassword;
  private volatile String bucketname;
  private final boolean deleteBucketOnClose;
  private final int numReplicas;
  private final String certsFile;
  private volatile boolean runWithTLS;
  private final String baseUrl;
  private final boolean isProtostellar;

  UnmanagedTestCluster(final Properties properties) {
    // localhost:8091 or couchbases://localhost:8091 or protostellar://localhost:8091 or protostellar://localhost
    String[] split = properties.getProperty("cluster.unmanaged.seed").split(":");
    isProtostellar = split[0].equals("couchbase2");
    seedHost = split[split.length - 2].replace("//", "");
    int seedPort = 0;
    try {
      seedPort = Integer.parseInt(split[split.length - 1]);
    }
    catch (NumberFormatException err) {
    }
    adminUsername = properties.getProperty("cluster.adminUsername");
    adminPassword = properties.getProperty("cluster.adminPassword");
    numReplicas = Integer.parseInt(properties.getProperty("cluster.unmanaged.numReplicas"));
    certsFile = properties.getProperty("cluster.unmanaged.certsFile");
    // protostellar is always TLS, runWithTLS for prostellar indicates to use https for OkHttpClient for setup
    runWithTLS = Boolean.parseBoolean(properties.getProperty("cluster.unmanaged.runWithTLS")) || seedPort == 18091;
    isDnsSrv = Boolean.parseBoolean(properties.getProperty("cluster.unmanaged.dnsSrv"));
    bucketname = Optional.ofNullable(properties.getProperty("cluster.unmanaged.bucket")).orElse("");
    deleteBucketOnClose = bucketname.isEmpty();
    httpClient = setupHttpClient(runWithTLS);
    baseUrl = (runWithTLS ? "https://" : "http://") + getNodeUrl(isDnsSrv, seedHost, runWithTLS) + ":" + (runWithTLS ? "18091" : "8091") ;
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
      .url(baseUrl + "/pools/default/b/" + urlEncode(bucketname))
      .build())
      .execute();

    String raw = getResponse.body().string();

    logger.info("Bucket raw results: {}", raw);

    waitUntilAllNodesHealthy();

    Response getClusterVersionResponse = httpClient.newCall(new Request.Builder()
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
      Map<Services,Integer> ports = Collections.emptyMap();
      nodeConfigs.add(new TestNodeConfig(seedHost, ports, false, Optional.of(DEFAULT_PROTOSTELLAR_TLS_PORT)));
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

  private Response httpGet(String relativeUrl) throws IOException {
    if (!relativeUrl.startsWith("/")) relativeUrl = "/" + relativeUrl;
    return httpClient.newCall(
        new Request.Builder()
          .url(baseUrl + relativeUrl)
          .build()
      )
      .execute();
  }


  private Optional<List<X509Certificate>> loadClusterCertificatesLegacy() {
    String path = "/pools/default/certificate";
    try (Response getResponse = httpGet(path)) {
      String raw = getResponse.body().string();
      int status = getResponse.code();

      if (status != 200) {
        logger.info("Could not load certificates from '{}'. httpStatus={} responseBody={}", path, status, raw);
        return Optional.empty();
      }

      return Optional.of(decodeCertificates(raw.getBytes(UTF_8)));

    } catch (Exception ex) {
      logger.error("Failed to decode certificates", ex);
      return Optional.empty();
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  private static class CertificateInfo {
    public String pem;
  }

  /**
   * Loads trusted CA certificates from the endpoint added in Couchbase Server 7.1.
   * This is the only way to get the certs from Couchbase Server 8.1 and later.
   */
  private Optional<List<X509Certificate>> loadClusterCertificatesModern() {
    String path = "/pools/default/trustedCAs";
    try (Response httpResponse = httpGet(path)) {
      String raw = httpResponse.body().string();
      int status = httpResponse.code();

      if (status != 200) {
        logger.info("Could not load certificates from '{}'. httpStatus={} responseBody={}", path, status, raw);
        return Optional.empty();
      }

      List<CertificateInfo> certs = jsonMapper.readValue(raw, new TypeReference<List<CertificateInfo>>() {});
      String mergedPem = certs.stream().map(it -> it.pem).collect(Collectors.joining("\r\n"));
      return Optional.of(decodeCertificates(mergedPem.getBytes(UTF_8)));

    } catch (Exception ex) {
      logger.error("Failed to decode certificates", ex);
      return Optional.empty();
    }
  }

  private Optional<List<X509Certificate>> loadClusterCertificate() {
    Optional<List<X509Certificate>> result = loadClusterCertificatesModern();
    if (!result.isPresent()) result = loadClusterCertificatesLegacy();
    return result;
  }

  private static List<X509Certificate> decodeCertificates(byte[] bytes) throws CertificateException {
    return decodeCertificates(new ByteArrayInputStream(bytes));
  }

  private static List<X509Certificate> decodeCertificates(InputStream is) throws CertificateException {
    //noinspection unchecked
    return new ArrayList<>((Collection<X509Certificate>) CertificateFactory.getInstance("X.509")
      .generateCertificates(is));
  }

  private Optional<List<X509Certificate>> loadMultipleRootCertsFromFile() {
    if (certsFile == null) return Optional.empty();

    try (FileInputStream fis = new FileInputStream(certsFile)) {
      return Optional.of(decodeCertificates(fis));
    } catch (Exception ex) {
      logger.error("Could not load certificates from '{}'", certsFile, ex);
      return Optional.empty();
    }
  }

  private void waitUntilAllNodesHealthy() throws Exception {
    while(true) {
      Response getResponse = httpGet("/pools/default/");
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
    if (deleteBucketOnClose) {
      try {
        httpClient.newCall(new Request.Builder()
          .url(baseUrl + "/pools/default/buckets/" + urlEncode(bucketname))
          .delete()
          .build()).execute();
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }
  }

  private OkHttpClient setupHttpClient(boolean useTLS) {
    OkHttpClient.Builder builder =  new OkHttpClient().newBuilder()
      .connectTimeout(30, TimeUnit.SECONDS)
      .readTimeout(30, TimeUnit.SECONDS)
      .writeTimeout(30, TimeUnit.SECONDS);

    builder.addInterceptor(chain -> {
      okhttp3.Request.Builder requestBuilder = chain.request().newBuilder()
        .addHeader("Authorization", Credentials.basic(adminUsername, adminPassword));
      return chain.proceed(requestBuilder.build());
    });

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

  /**
   * Whether the test config has asked to connect to the cluster over protostellar://
   *
   */
  @Override
  public boolean isProtostellar() {
    return isProtostellar;
  }

}
