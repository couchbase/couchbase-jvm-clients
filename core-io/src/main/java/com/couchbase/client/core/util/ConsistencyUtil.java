/*
 * Copyright 2022 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.core.util;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.config.PortInfo;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpMethod;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import com.couchbase.client.core.endpoint.http.CoreHttpPath;
import com.couchbase.client.core.endpoint.http.CoreHttpRequest;
import com.couchbase.client.core.endpoint.http.CoreHttpResponse;
import com.couchbase.client.core.error.HttpStatusCodeException;
import com.couchbase.client.core.error.IndexNotFoundException;
import com.couchbase.client.core.error.RequestCanceledException;
import com.couchbase.client.core.error.ViewServiceException;
import com.couchbase.client.core.msg.CancellationReason;
import com.couchbase.client.core.msg.RequestTarget;
import com.couchbase.client.core.node.NodeIdentifier;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.transaction.util.TriFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.couchbase.client.core.deps.com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static com.couchbase.client.core.endpoint.http.CoreHttpPath.path;
import static com.couchbase.client.core.util.CbCollections.mapOf;

/**
 * Helper routines for tests to wait for all nodes to have a consistent view of a resource.
 */
@Stability.Internal
public class ConsistencyUtil {
  // MB50101 adds a rest API endpoint for search that allows querying the status of a particular index.
  public static final String CLUSTER_VERSION_MB_50101 = "7.1.0";

  private final static ObjectMapper mapper = new ObjectMapper().disable(FAIL_ON_UNKNOWN_PROPERTIES);

  static class Collection {
    public String name;
  }

  static class Scope {
    public String name;
    public List<Collection> collections;
  }

  static class ScopesResponse {
    public List<Scope> scopes;
  }

  private static final Logger logger = LoggerFactory.getLogger(ConsistencyUtil.class);

  public static CoreHttpPath pathForUser(String domain, String name) {
    return CoreHttpPath.path("/settings/rbac/users/{domain}/{name}", mapOf("domain", domain, "name", name));
  }

  public static CoreHttpPath pathForGroup(String name) {
    return CoreHttpPath.path("/settings/rbac/groups/{name}", mapOf("name", name));
  }

  public static CoreHttpPath pathForBucket(String name) {
    return path("/pools/default/buckets/{bucketName}", mapOf("bucketName", name));
  }

  private static CoreHttpPath pathForScopes(String bucketName) {
    return path("/pools/default/buckets/{bucketName}/scopes", mapOf("bucketName", bucketName));
  }

  public static CoreHttpPath pathForView(String bucketName, String designDocument, String viewName) {
    return CoreHttpPath.path("/{bucketName}/_design/{designDocument}/_view/{viewName}",
      mapOf("bucketName", bucketName, "designDocument", designDocument, "viewName", viewName));
  }

  public static CoreHttpPath pathForDesignDocument(String bucketName, String designDocument) {
    return CoreHttpPath.path("/{bucketName}/_design/{designDocument}",
      mapOf("bucketName", bucketName, "designDocument", designDocument));
  }

  public static CoreHttpPath pathForSearchIndex(String indexName) {
    return CoreHttpPath.path("/api/index/{indexName}",
      mapOf("indexName", indexName));
  }

  private static RequestTarget defaultManagerTarget(NodeIdentifier node) {
    return new RequestTarget(ServiceType.MANAGER, node, null);
  }

  private static CoreHttpRequest defaultManagerRequest(Core core, CoreHttpPath path, NodeIdentifier node) {
    RequestTarget target = defaultManagerTarget(node);
    return CoreHttpRequest.builder(CoreCommonOptions.DEFAULT, core.context(), HttpMethod.GET, path, target).build();
  }

  public static void waitUntilUserPresent(Core core, String domain, String name) {
    waitUntilAllNodesHaveSameStatus(core, pathForUser(domain, name), 200);
  }

  public static void waitUntilUserDropped(Core core, String domain, String name) {
    waitUntilAllNodesHaveSameStatus(core, pathForUser(domain, name), 404);
  }

  public static void waitUntilGroupPresent(Core core, String name) {
    waitUntilAllNodesHaveSameStatus(core, pathForGroup(name), 200);
  }

  public static void waitUntilGroupDropped(Core core, String name) {
    waitUntilAllNodesHaveSameStatus(core, pathForGroup(name), 404);
  }

  public static void waitUntilBucketPresent(Core core, String name) {
    waitUntilAllNodesHaveSameStatus(core, pathForBucket(name), 200);
  }

  public static void waitUntilBucketDropped(Core core, String name) {
    waitUntilAllNodesHaveSameStatus(core, pathForBucket(name), 404);
  }

  public static void waitUntilDesignDocumentPresent(Core core, String bucketName, String designDocument) {
    waitUntilAllNodesHaveSameStatusViews(core, pathForDesignDocument(bucketName, designDocument), 200, bucketName);
  }

  public static void waitUntilDesignDocumentDropped(Core core, String bucketName, String designDocument) {
    waitUntilAllNodesHaveSameStatusViews(core, pathForDesignDocument(bucketName, designDocument), 404, bucketName);
  }

  public static void waitUntilViewPresent(Core core, String bucketName, String designDocument, String viewName) {
    waitUntilAllNodesHaveSameStatusViews(core, pathForView(bucketName, designDocument, viewName), 200, bucketName);
  }

  public static void waitUntilViewDropped(Core core, String bucketName, String designDocument, String viewName) {
    waitUntilAllNodesHaveSameStatusViews(core, pathForView(bucketName, designDocument, viewName), 404, bucketName);
  }

  private static ScopesResponse convertScopesResponse(CoreHttpResponse response) {
    try {
      return mapper.reader().readValue(response.content(), ScopesResponse.class);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static void waitUntilScopePresent(Core core, String bucketName, String scopeName) {
    CoreHttpPath path = pathForScopes(bucketName);
    waitUntilAllNodesMatchPredicate(core, (statusCode, response, err) -> {
        if (err != null) throw err;
        if (statusCode != 200) return false;

        return convertScopesResponse(response).scopes.stream().anyMatch(v -> v.name.equals(scopeName));
      }, "scope " + scopeName + " present",
      (node) -> defaultManagerRequest(core, path, node));
  }

  public static void waitUntilScopeDropped(Core core, String bucketName, String scopeName) {
    CoreHttpPath path = pathForScopes(bucketName);
    waitUntilAllNodesMatchPredicate(core, (statusCode, response, err) -> {
        if (err != null) throw err;
        if (statusCode != 200) return false;

        return convertScopesResponse(response).scopes.stream().noneMatch(v -> v.name.equals(scopeName));
      }, "scope " + scopeName + " dropped",
      (node) -> defaultManagerRequest(core, path, node));
  }

  public static void waitUntilCollectionPresent(Core core, String bucketName, String scopeName, String collectionName) {
    CoreHttpPath path = pathForScopes(bucketName);
    waitUntilAllNodesMatchPredicate(core, (statusCode, response, err) -> {
        if (err != null) throw err;
        if (statusCode != 200) return false;

        return convertScopesResponse(response).scopes.stream().anyMatch(v -> v.name.equals(scopeName)
          && v.collections.stream().anyMatch(coll -> coll.name.equals(collectionName)));
      }, "collection " + scopeName + "." + collectionName + " exists",
      (node) -> defaultManagerRequest(core, path, node));
  }

  public static void waitUntilCollectionDropped(Core core, String bucketName, String scopeName, String collectionName) {
    CoreHttpPath path = pathForScopes(bucketName);
    waitUntilAllNodesMatchPredicate(core, (statusCode, response, err) -> {
        if (err != null) throw err;
        if (statusCode != 200) return false;

        boolean anyMatch = convertScopesResponse(response).scopes.stream().anyMatch(v -> v.name.equals(scopeName)
          && v.collections.stream().anyMatch(coll -> coll.name.equals(collectionName)));
        return !anyMatch;
      }, "collection " + scopeName + "." + collectionName + " dropped",
      (node) -> defaultManagerRequest(core, path, node));
  }

  // Relies on a REST API added in 7.1.0 (MB-50101)
  public static void waitUntilSearchIndexPresent(Core core, String indexName) {
    waitUntilAllNodesHaveSameStatusSearch(core, pathForSearchIndex(indexName), 200);
  }

  // Relies on a REST API added in 7.1.0 (MB-50101)
  public static void waitUntilSearchIndexDropped(Core core, String indexName) {
    waitUntilAllNodesHaveSameStatusSearch(core, pathForSearchIndex(indexName), 404);
  }

  private static void waitUntilAllNodesHaveSameStatus(Core core, CoreHttpPath path, int requiredHttpStatus) {
    waitUntilAllNodesMatchPredicate(core,
      (statusCode, response, err) -> {
        if (err != null) throw err;
        return statusCode == requiredHttpStatus;
      },
      "status == " + requiredHttpStatus,
      (node) -> defaultManagerRequest(core, path, node));
  }

  private static void waitUntilAllNodesHaveSameStatusSearch(Core core, CoreHttpPath path, int requiredHttpStatus) {
    // Temporarily disabled as CI is (intermittently?) hitting TARGET_NODE_REMOVED issues that persist on looping and fetching a fresh config.  Requires the diagnostics patch to debug.

//    waitUntilAllNodesMatchPredicate(core,
//      (statusCode, response, err) -> {
//        if (err instanceof IndexNotFoundException) {
//          // Emulate this as a 404
//          return requiredHttpStatus == 404;
//        }
//        if (err != null) throw err;
//        return statusCode == requiredHttpStatus;
//      },
//      "status views == " + requiredHttpStatus,
//      (node) -> {
//        RequestTarget target = new RequestTarget(ServiceType.SEARCH, node, null);
//        return CoreHttpRequest.builder(CoreCommonOptions.DEFAULT, core.context(), HttpMethod.GET, path, target).build();
//      });
  }

  private static void waitUntilAllNodesHaveSameStatusViews(Core core, CoreHttpPath path, int requiredHttpStatus, String bucketName) {
    waitUntilAllNodesMatchPredicate(core,
      (statusCode, response, err) -> {
        if (err instanceof ViewServiceException) {
          if (err.getMessage().contains("\"error\":\"not_found\"")) {
            // Emulate this as a 404
            return requiredHttpStatus == 404;
          }
          throw err;
        }
        if (err != null) throw err;
        return statusCode == requiredHttpStatus;
      },
      String.format("view %s == %d", path.format(), requiredHttpStatus),
      (node) -> {
        RequestTarget target = new RequestTarget(ServiceType.VIEWS, node, bucketName);
        return CoreHttpRequest.builder(CoreCommonOptions.DEFAULT, core.context(), HttpMethod.GET, path, target).build();
      });
  }

  private static Set<NodeIdentifier> getConfig(Core core) {
    Set<NodeIdentifier> ret = new HashSet<>();

    if (core.clusterConfig().globalConfig() != null) {
      List<NodeIdentifier> nodes = core.clusterConfig().globalConfig().portInfos()
        .stream()
        .map(PortInfo::identifier)
        .collect(Collectors.toList());

      logger.info("Adding nodes from global config: {}", nodes);

      ret.addAll(nodes);
    }

    // GCCCP was added in 6.5, make sure compatible with previous cluster versions
    if (core.clusterConfig().bucketConfigs() != null) {
      List<NodeIdentifier> nodes = core.clusterConfig().bucketConfigs().entrySet()
        .stream()
        .flatMap(v -> v.getValue().nodes().stream().map(x -> x.identifier()))
        .collect(Collectors.toList());

      logger.info("Adding nodes from bucket configs: {}", nodes);

      ret.addAll(nodes);
    }

    return ret;
  }

  private static Set<NodeIdentifier> waitForConfig(Core core) {
    logger.info("Waiting for config");

    long start = System.nanoTime();

    while (true) {
      try {
        Set<NodeIdentifier> config = getConfig(core);

        if (!config.isEmpty()) {
          return config;
        }

        try {
          Thread.sleep(10);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      } catch (RuntimeException err) {
        // The above code isn't thread-safe and the config can be changing under us.  E.g. we may pass a check that the global config is not null, and then when we go to use it, it is.
        // Not much we can do but just try to catch any fall-out and retry.
        logger.info("Ignoring error {} while getting config", err.toString());
      }

      if (TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start) > 30) {
        throw new RuntimeException("Timeout waiting for config");
      }
    }
  }

  private static void waitUntilAllNodesMatchPredicate(Core core,
                                                      TriFunction<Integer, CoreHttpResponse, RuntimeException, Boolean> onResult,
                                                      String predicateDesc,
                                                      Function<NodeIdentifier, CoreHttpRequest> createRequest) {
    Set<NodeIdentifier> portInfos = waitForConfig(core);

    long start = System.nanoTime();

    for (NodeIdentifier node : portInfos) {
      boolean done = false;

      while (!done) {

        String debug = String.format("%s:%d waiting for %s", node.address(), node.managerPort(), predicateDesc);

        CoreHttpRequest request = createRequest.apply(node);

        logger.debug("Querying {} running", debug);

        try {
          CoreHttpResponse response = request.exec(core).get();

          done = onResult.apply(response.httpStatus(), response, null);

          logger.debug("Querying {}: {} {} {}", debug, response.httpStatus(), response, done);
        } catch (ExecutionException e) {
          logger.debug("Querying {}: {}", debug, e.toString());

          if (e.getCause() instanceof RequestCanceledException) {
            // Can see these errors with stacktrace and retry reasons containing handleTargetNotAvailable and TARGET_NODE_REMOVED, respectively.
            // Somewhat unclear why, but end result is that the node has gone, so we need to get an updated config and start again.
            if (((RequestCanceledException) e.getCause()).reason() == CancellationReason.TARGET_NODE_REMOVED) {
              try {
                Thread.sleep(200);
              } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
              }
              waitUntilAllNodesMatchPredicate(core, onResult, predicateDesc, createRequest);
              return;
            }
          }
          else if (e.getCause() instanceof HttpStatusCodeException) {
            int statusCode = ((HttpStatusCodeException) e.getCause()).httpStatusCode();
            logger.debug("Querying {}: {}", debug, statusCode);

            done = onResult.apply(statusCode, null, null);
          } else if (e.getCause() instanceof RuntimeException) {
            // We're using CoreHttpRequest, and the response gets passed through various layers.  E.g. can see ViewServiceException when doing a view request.
            done = onResult.apply(null, null, (RuntimeException) e.getCause());
          } else {
            throw new RuntimeException(e.getCause());
          }
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }

        if (!done) {
          try {
            Thread.sleep(10);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }

          if (TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start) > 30) {
            throw new RuntimeException(debug + " timeout");
          }
        }
      }
    }
  }
}
