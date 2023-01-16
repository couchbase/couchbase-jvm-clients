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
package com.couchbase.client.test;

public enum Services {
  KV("kv"),
  KV_TLS("kvSSL"),
  MANAGER("mgmt"),
  MANAGER_TLS("mgmtSSL"),
  QUERY("n1ql"),
  QUERY_TLS("n1qlSSL"),
  ANALYTICS("cbas"),
  ANALYTICS_TLS("cbasSSL"),
  SEARCH("fts"),
  SEARCH_TLS("ftsSSL"),
  VIEW("capi"),
  VIEW_TLS("capiSSL"),
  EVENTING("eventing"),
  EVENTING_TLS("eventingSSL");
  private final String nodeName;

  Services(String kv) {
    this.nodeName = kv;
  }

  public String getNodeName() {
    return nodeName;
  }
}
