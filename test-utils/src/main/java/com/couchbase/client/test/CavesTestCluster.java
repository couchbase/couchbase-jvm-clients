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
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class CavesTestCluster extends TestCluster {
  private static final Logger LOGGER = LoggerFactory.getLogger(CavesTestCluster.class);
  private static final Pattern IPV_6_PATTERN = Pattern.compile("^\\[(.+)]:(\\d+(=\\w+)?)$");
  private static final String DEFAULT_CAVES_VERSION = "v0.0.1-71";

  private static final Function<String, String> REMOVE_SCHEMA = str -> str.replaceAll("\\w+://", "");
  private static final Function<String, String> REMOVE_PARAMS = str -> str.replaceAll("\\?.*", "");
  private static final Function<String, String> REMOVE_USERNAME = str -> str.replaceAll(".*@", "");
  private static final Function<String, String> CLEANUP = str -> REMOVE_SCHEMA
    .andThen(REMOVE_USERNAME)
    .andThen(REMOVE_PARAMS)
    .apply(str);
  public static final String DEFAULT_BINARY_NAME = "gocaves-linux";

  private volatile CavesProcess cavesProcess;

  private final CavesControlServer controlServer;

  private final String testId;
  private final Properties properties;

  CavesTestCluster(final Properties properties) {
    this.properties = properties;
    this.testId = UUID.randomUUID().toString();

    this.controlServer = new CavesControlServer();
    this.baseUrl = "http://%s:%s";
    this.adminUsername = "Administrator";
    this.adminPassword = "password";
    httpClient = new OkHttpClient.Builder()
      .connectTimeout(30, TimeUnit.SECONDS)
      .readTimeout(30, TimeUnit.SECONDS)
      .writeTimeout(30, TimeUnit.SECONDS)
      .build();
  }

  @Override
  TestClusterConfig _start() throws Exception {
    LOGGER.info("Starting CAVES");

    Path binPath = getCavesBinary();

    controlServer.start();

    cavesProcess = new CavesProcess(binPath, controlServer.port());
    cavesProcess.start();

    controlServer.receivedHello().get(10, TimeUnit.SECONDS);

    LOGGER.debug("Received hello from CAVES, proceeding with cluster setup.");

    Map<String, Object> response = controlServer.startTesting(testId, "java-sdk");
    String connstr = (String) response.get("connstr");
    List<String> mgmtAddrs = (List<String>) response.get("mgmt_addrs");

    LOGGER.info("CAVES connection string is {}", connstr);

    List<UnresolvedSocket> mgmtSockets = mgmtAddrs.stream().flatMap(s -> parseHosts(s).stream()).collect(Collectors.toList());

    String host = mgmtSockets.get(0).hostname;
    int port = mgmtSockets.get(0).port;
    this.baseUrl = String.format(baseUrl, host, port);
    Request.Builder builder = builderWithAuth();

    String rawConfig = getRawConfig(builder);
    ClusterVersion clusterVersion = getClusterVersionFromServer(builder);

    return new TestClusterConfig(
      bucketname,
      adminUsername,
      adminPassword,
      nodesFromRaw(host, rawConfig),
      replicasFromRaw(rawConfig),
      Optional.empty(),
      capabilitiesFromRaw(rawConfig, clusterVersion),
      clusterVersion,
      false
    );
  }

  private Path getCavesBinary() throws IOException {
    DetectedOs os = DetectedOs.detectOs(properties);
    String binaryName = Optional.ofNullable(os).map(DetectedOs::getBinaryName).orElse(DEFAULT_BINARY_NAME);

    String cavesVersion = properties.getProperty("cluster.caves.version", DEFAULT_CAVES_VERSION);

    LOGGER.info("CAVES version: " + cavesVersion);

    String downloadUrl = "https://github.com/couchbaselabs/gocaves/releases/download/"+ cavesVersion +"/" + binaryName;

    String tmpDir = properties.getProperty("java.io.tmpdir");
    String binaryWithVersion = binaryName + "-" + cavesVersion;
    Path binPath = Paths.get(tmpDir, binaryWithVersion);

    if (Files.exists(binPath)) {
      LOGGER.debug("CAVES binary with path {} already exists, not re-downloading.", binPath);
      return binPath.toAbsolutePath();
    }

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

    return binPath.toAbsolutePath();
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

  enum DetectedOs {
    Windows("win","gocaves-windows.exe"),
    Linux(null,DEFAULT_BINARY_NAME),
    Macos("mac","gocaves-macos");
    private final String osPrefix;
    private final String binaryName;

    DetectedOs(String osPrefix, String binaryName) {
      this.osPrefix = osPrefix;
      this.binaryName = binaryName;
    }

    public String getBinaryName() {
      return binaryName;
    }

    public static DetectedOs detectOs(Properties properties) {
      String osName = properties.getProperty("os.name").toLowerCase(Locale.ROOT);
      return Arrays.stream(DetectedOs.values())
        .filter(val -> val.osPrefix != null && osName.contains(val.osPrefix))
        .findFirst()
        .orElse(DetectedOs.Linux);
    }
  }

  private static List<UnresolvedSocket> parseHosts(final String input) {
    String[] splitted = CLEANUP.apply(input).split(",");

    List<UnresolvedSocket> hosts = new ArrayList<>();

    for (String singleHost : splitted) {
      if (singleHost == null || singleHost.isEmpty()) {
        continue;
      }
      singleHost = singleHost.trim();

      Matcher matcher = IPV_6_PATTERN.matcher(singleHost);
      if (isIPv6Address(singleHost)) {
        // this is an ipv6 addr!
        singleHost = singleHost.substring(1, singleHost.length() - 1);
        hosts.add(createHost(singleHost, 0));
      } else if (matcher.matches()) {
        // this is ipv6 with addr and port!
        String rawPort = matcher.group(2);
        String host = matcher.group(1);
        if (rawPort.contains("=")) {
          String[] portParts = rawPort.split("=");
          hosts.add(createHost(host, Integer.parseInt(portParts[0]),
            PortType.fromString(portParts[1])));
        } else {
          hosts.add(createHost(host, Integer.parseInt(matcher.group(2))));
        }
      } else {
        // either ipv4 or a hostname
        String[] parts = singleHost.split(":");
        String host = parts[0];
        if (parts.length == 1) {
          hosts.add(createHost(host, 0));
        } else {
          if (parts[1].contains("=")) {
            // has custom port type
            String[] portParts = parts[1].split("=");
            hosts.add(createHost(host,
              Integer.parseInt(portParts[0]),
              PortType.fromString(portParts[1]))
            );
          } else {
            int port = Integer.parseInt(parts[1]);
            hosts.add(createHost(host, port));
          }
        }
      }
    }
    return hosts;
  }

  private static UnresolvedSocket createHost(String host, int port, PortType portType) {
    return new UnresolvedSocket(host, port, portType);
  }
  private static UnresolvedSocket createHost(String host, int port) {
    return new UnresolvedSocket(host, port, null);
  }

  private static boolean isIPv6Address(String singleHost) {
    return singleHost.startsWith("[") && singleHost.endsWith("]");
  }

  static class UnresolvedSocket {

    private final String hostname;
    private final int port;
    private final PortType portType;

    UnresolvedSocket(String hostname, int port, PortType portType) {
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

    public PortType portType() {
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
    MANAGER("http", "manager"),
    KV("mcd", "kv");
    private final List<String> values;

    PortType(String... values) {
      this.values = Arrays.asList(values);
    }

    public List<String> getValues() {
      return values;
    }

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
      return Arrays.stream(PortType.values())
        .filter(portType -> portType.getValues().contains(input.toLowerCase()))
        .findFirst()
        .orElseThrow(() -> new RuntimeException("Unsupported port type \"" + input + "\""));
    }
  }

}
