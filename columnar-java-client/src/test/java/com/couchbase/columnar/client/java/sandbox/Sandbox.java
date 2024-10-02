/*
 * Copyright 2024 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.columnar.client.java.sandbox;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.columnar.client.java.Cluster;
import com.couchbase.columnar.client.java.Credential;
import com.couchbase.columnar.client.java.Database;
import com.couchbase.columnar.client.java.QueryResult;
import com.couchbase.columnar.client.java.Scope;
import com.couchbase.columnar.client.java.internal.Certificates;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Arrays;
import java.util.List;

import static com.couchbase.client.core.util.CbCollections.listOf;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public class Sandbox {

  public static void main(String[] args) throws Exception {
    String connectionString = "couchbases://127.0.0.1?security.disable_server_certificate_verification=true&srv=0";
    String username = "Administrator";
    String password = "password";

    try (Cluster cluster = Cluster.newInstance(
      connectionString,
      Credential.of(username, password),
      clusterOptions -> clusterOptions
        .security(it -> it.trustOnlyCertificates(Certificates.getNonProdCertificates()))
    )) {

      QueryResult result = cluster.executeQuery(
        "select ?=1",
        options -> options
          .readOnly(true)
          .parameters(listOf(1))
      );
      result.rows().forEach(System.out::println);

      // Same again, but streaming.
      //
      // NOTE: 'row' callback is always second parameter,
      // even though it would be ideal to keep the options
      // closer to the statement since they "happen" first.
      // However, if `row` callback comes last, IntelliJ's
      // auto-complete gets confused by the overloads.
      cluster.executeStreamingQuery(
        "select ?=1",
        row -> System.out.println(row),
        options -> options
          .readOnly(true)
          .parameters(listOf(1))
      );

      fetchDatabases(cluster).forEach(System.out::println);
    }
  }

  private static void createIfAbsent(Scope scope) {
    createIfAbsent(scope.database());
    if (!scope.name().equals("Default")) {
      scope.database().cluster().executeQuery("CREATE SCOPE " + quote(scope.database().name(), scope.name()) + " IF NOT EXISTS");
    }
  }


  private static void createIfAbsent(Database database) {
    if (!database.name().equals("Default")) {
      database.cluster().executeQuery("CREATE DATABASE " + quote(database.name()) + " IF NOT EXISTS");
    }
  }

  static String quote(String... nameComponents) {
    for (String s : nameComponents) {
      if (s.contains("`")) {
        throw new IllegalArgumentException("Invalid name for a database object: " + s + " (must not contain backticks)");
      }
    }
    return Arrays.stream(nameComponents)
      .map(it -> "`" + it + "`")
      .collect(joining("."));
  }

  @Stability.Volatile
  public static List<DatabaseMetadata> fetchDatabases(Cluster cluster) {
    return cluster.executeQuery(
      "SELECT RAW {`DatabaseName`, `SystemDatabase`}" +
        " FROM `System`.`Metadata`.`Database`",
      opt -> opt.readOnly(true)
    )
      .rows().stream()
      .map(row -> row.as(ObjectNode.class))
      .map(node -> new DatabaseMetadata(
        node.path("DatabaseName").textValue(),
        node.path("SystemDatabase").booleanValue())
      )
      .collect(toList());
  }

  public static List<ScopeMetadata> fetchScopes(Database database) {
    return database.cluster().executeQuery(
        "SELECT RAW `DataverseName`" +
          " FROM `System`.`Metadata`.`Dataverse`" +
          " WHERE `DatabaseName` = ?",
        opt -> opt
          .parameters(listOf(database.name()))
          .readOnly(true)
      )
      .rows().stream()
      .map(row -> row.as(String.class))
      .map(ScopeMetadata::new)
      .collect(toList());
  }



  @Stability.Volatile
  public List<String> viewNames(Scope scope) {
    return scope.database().cluster().executeQuery(
        "SELECT RAW DatasetName FROM Metadata.`Dataset` WHERE DatasetType = 'VIEW' AND DataverseName = ?",
        opts -> opts
          .readOnly(true)
          .parameters(listOf(scope.name()))
      )
      .rows().stream()
      .map(row -> row.as(String.class))
      .collect(toList());
  }

  @Stability.Volatile
  public void createStandaloneCollection(Scope scope, String name, PrimaryKey primaryKey) {
    scope.executeQuery("CREATE COLLECTION " + quote(name) + " " + primaryKey.toSql());
  }

}
