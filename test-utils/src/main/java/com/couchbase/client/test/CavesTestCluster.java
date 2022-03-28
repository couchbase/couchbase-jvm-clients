/*
 * Copyright (c) 2021 Couchbase, Inc.
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

import com.couchbase.client.test.caves.CavesControlServer;
import com.couchbase.client.test.caves.CavesProcess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.okhttp3.Credentials;
import org.testcontainers.shaded.okhttp3.OkHttpClient;
import org.testcontainers.shaded.okhttp3.Request;
import org.testcontainers.shaded.okhttp3.Response;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class CavesTestCluster extends TestCluster {

  private static final String CAVES_VERSION = "v0.0.1-71";

  private static final Logger LOGGER = LoggerFactory.getLogger(CavesTestCluster.class);

  private volatile CavesProcess cavesProcess;

  private final CavesControlServer controlServer;

  private final String testId;

  private final OkHttpClient httpClient = new OkHttpClient.Builder()
    .connectTimeout(30, TimeUnit.SECONDS)
    .readTimeout(30, TimeUnit.SECONDS)
    .writeTimeout(30, TimeUnit.SECONDS)
    .build();

  private final Properties properties;

  CavesTestCluster(final Properties properties) {
    this.properties = properties;
    this.testId = UUID.randomUUID().toString();

    this.controlServer = new CavesControlServer();
  }

  @Override
  TestClusterConfig _start() throws Exception {
    LOGGER.info("Starting CAVES");

    DetectedOs os = detectOs();
    String binaryName;
    switch (os) {
      case Macos:
        binaryName = "gocaves-macos";
        break;
      case Windows:
        binaryName = "gocaves-windows.exe";
        break;
      default:
        binaryName = "gocaves-linux";
    }

    String cavesVersion = properties.getProperty("cluster.caves.version", CAVES_VERSION);

    LOGGER.info("CAVES version: " + cavesVersion);

    String downloadUrl = "https://github.com/couchbaselabs/gocaves/releases/download/"+ cavesVersion +"/" + binaryName;

    String tmpDir = properties.getProperty("java.io.tmpdir");
    String binaryWithVersion = binaryName + "-" + cavesVersion;
    Path binPath = Paths.get(tmpDir, binaryWithVersion);

    if (!Files.exists(binPath)) {
      LOGGER.info("CAVES binary with path {} does not exist, downloading...", binPath);
      LOGGER.debug("Downloading from {}", downloadUrl);

      Request request = new Request.Builder().url(downloadUrl).build();
      Response response = httpClient.newCall(request).execute();

      InputStream inputStream = response.body().byteStream();
      Files.copy(inputStream, binPath);
      response.body().close();

      Set<PosixFilePermission> perms = new HashSet<>();
      perms.add(PosixFilePermission.OWNER_EXECUTE);
      Files.setPosixFilePermissions(binPath, perms);

      LOGGER.debug("Completed downloading the CAVES binary");
    } else {
      LOGGER.debug("CAVES binary with path {} already exists, not re-downloading.", binPath);
    }

    controlServer.start();

    cavesProcess = new CavesProcess(binPath.toAbsolutePath(), controlServer.port());
    cavesProcess.start();

    controlServer.receivedHello().get(10, TimeUnit.SECONDS);

    LOGGER.debug("Received hello from CAVES, proceeding with cluster setup.");


    Map<String, Object> response = controlServer.startTesting(testId, "java-sdk");
    String connstr = (String) response.get("connstr");
    List<String> mgmtAddrs = (List<String>) response.get("mgmt_addrs");

    LOGGER.info("CAVES connection string is {}", connstr);

    List<UnresolvedSocket> mgmtSockets = mgmtAddrs.stream().flatMap(s -> parseHosts(s).stream()).collect(Collectors.toList());

    String bucketname = "default";
    String username = "Administrator";
    String password = "password";

    Response getResponse = httpClient.newCall(new Request.Builder()
      .header("Authorization", Credentials.basic(username, password))
      .url("http://" + mgmtSockets.get(0).hostname + ":" + mgmtSockets.get(0).port + "/pools/default/b/" + bucketname)
      .build())
      .execute();

    String raw = getResponse.body().string();

    Response getClusterVersionResponse = httpClient.newCall(new Request.Builder()
      .header("Authorization", Credentials.basic(username, password))
      .url("http://" + mgmtSockets.get(0).hostname + ":" + mgmtSockets.get(0).port + "/pools")
      .build())
      .execute();

    ClusterVersion clusterVersion = parseClusterVersion(getClusterVersionResponse);

    return new TestClusterConfig(
      bucketname,
      username,
      password,
      nodesFromRaw(mgmtSockets.get(0).hostname, raw),
      replicasFromRaw(raw),
      Optional.empty(),
      capabilitiesFromRaw(raw, clusterVersion),
      clusterVersion,
      false
    );
  }

  public Map<String, Object> endTesting() throws Exception {
    return controlServer.endTesting(testId);
  }

  public Map<String, Object> startTest(String testName) throws Exception {
    return controlServer.startTest(testId, testName);
  }

  public Map<String, Object> endTest() throws Exception {
    return controlServer.endTest(testId);
  }

  @Override
  ClusterType type() {
    return ClusterType.CAVES;
  }

  @Override
  public void close() throws Throwable {
    cavesProcess.stop();
  }

  private DetectedOs detectOs() {
    String osName = properties.getProperty("os.name").toLowerCase(Locale.ROOT);

    if (osName.contains("mac")) {
      return DetectedOs.Macos;
    } else if (osName.contains("win")) {
      return DetectedOs.Windows;
    } else {
      return DetectedOs.Linux;
    }
  }

  enum DetectedOs {
    Windows,
    Linux,
    Macos
  }

  private static List<UnresolvedSocket> parseHosts(final String input) {
    String schemeRemoved = input.replaceAll("\\w+://", "");
    String usernameRemoved = schemeRemoved.replaceAll(".*@", "");
    String paramsRemoved = usernameRemoved.replaceAll("\\?.*", "");
    String[] splitted = paramsRemoved.split(",");

    List<UnresolvedSocket> hosts = new ArrayList<>();

    Pattern ipv6pattern = Pattern.compile("^\\[(.+)]:(\\d+(=\\w+)?)$");
    for (int i = 0; i < splitted.length; i++) {
      String singleHost = splitted[i];
      if (singleHost == null || singleHost.isEmpty()) {
        continue;
      }
      singleHost = singleHost.trim();

      Matcher matcher = ipv6pattern.matcher(singleHost);
      if (singleHost.startsWith("[") && singleHost.endsWith("]")) {
        // this is an ipv6 addr!
        singleHost = singleHost.substring(1, singleHost.length() - 1);
        hosts.add(new UnresolvedSocket(singleHost, 0, Optional.empty()));
      } else if (matcher.matches()) {
        // this is ipv6 with addr and port!
        String rawPort = matcher.group(2);
        if (rawPort.contains("=")) {
          String[] portParts = rawPort.split("=");
          hosts.add(new UnresolvedSocket(
            matcher.group(1),
            Integer.parseInt(portParts[0]),
            Optional.of(PortType.fromString(portParts[1])))
          );
        } else {
          hosts.add(new UnresolvedSocket(
            matcher.group(1),
            Integer.parseInt(matcher.group(2)),
            Optional.empty()
          ));
        }
      } else {
        // either ipv4 or a hostname
        String[] parts = singleHost.split(":");
        if (parts.length == 1) {
          hosts.add(new UnresolvedSocket(parts[0], 0, Optional.empty()));
        } else {
          if (parts[1].contains("=")) {
            // has custom port type
            String[] portParts = parts[1].split("=");
            hosts.add(new UnresolvedSocket(
              parts[0],
              Integer.parseInt(portParts[0]),
              Optional.of(PortType.fromString(portParts[1])))
            );
          } else {
            int port = Integer.parseInt(parts[1]);
            hosts.add(new UnresolvedSocket(parts[0], port, Optional.empty()));
          }
        }
      }
    }
    return hosts;
  }

  static class UnresolvedSocket {

    private final String hostname;
    private final int port;
    private final Optional<PortType> portType;

    UnresolvedSocket(String hostname, int port, Optional<PortType> portType) {
      this.hostname = hostname;
      this.port = port;
      this.portType = portType;
    }

    public String hostname() {
      return hostname;
    }

    public int port() {
      return port;
    }

    public Optional<PortType> portType() {
      return portType;
    }

    @Override
    public String toString() {
      return "UnresolvedSocket{" +
        "hostname='" + hostname + '\'' +
        ", port=" + port +
        ", portType=" + portType +
        '}';
    }
  }

  enum PortType {
    MANAGER,
    KV;

    /**
     * Turn the raw representation into an enum.
     * <p>
     * Note that we support both "http" and "mcd" from libcouchbase to be compatible, but also expose "manager"
     * and "kv" so it more aligns with the current terminology of services.
     *
     * @param input the raw representation from the connstr.
     * @return the enum if it could be determined.
     */
    static PortType fromString(final String input) {
      if (input.equalsIgnoreCase("http") || input.equalsIgnoreCase("manager")) {
        return PortType.MANAGER;
      } else if (input.equalsIgnoreCase("mcd") || input.equalsIgnoreCase("kv")) {
        return PortType.KV;
      } else {
        throw new RuntimeException("Unsupported port type \"" + input + "\"");
      }
    }
  }

}
