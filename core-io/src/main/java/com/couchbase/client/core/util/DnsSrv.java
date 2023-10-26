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

package com.couchbase.client.core.util;

import com.couchbase.client.core.annotation.Stability;

import javax.naming.NameNotFoundException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ForkJoinPool;

import static com.couchbase.client.core.util.CbCollections.transform;

/**
 * The default implementation for performing DNS SRV lookups.
 */
@Stability.Internal
public class DnsSrv {

  /**
   * The default DNS prefix for not encrypted connections.
   */
  public static final String DEFAULT_DNS_SERVICE = "_couchbase._tcp.";

  /**
   * The default DNS prefix for encrypted connections.
   */
  public static final String DEFAULT_DNS_SECURE_SERVICE = "_couchbases._tcp.";

  private DnsSrv() {
  }

  /**
   * Fetch a bootstrap list from DNS SRV using default OS name resolution.
   *
   * @param serviceName the DNS SRV locator.
   * @param full if the service name is the full one or needs to be enriched by the couchbase prefixes.
   * @param secure if the secure service prefix should be used.
   * @return a list of DNS SRV records.
   * @throws NameNotFoundException if there's no SRV record associated with serviceName
   */
  public static List<String> fromDnsSrv(final String serviceName, boolean full, boolean secure) throws NameNotFoundException {
    String fullService;
    if (full) {
      fullService = serviceName;
    } else {
      fullService = (secure ? DEFAULT_DNS_SECURE_SERVICE : DEFAULT_DNS_SERVICE) + serviceName;
    }

    try {
      DnsSrvResolver resolver = new DnsSrvResolver(ForkJoinPool.commonPool());
      List<HostAndPort> results = resolver.resolve(fullService)
        .blockOptional().orElseThrow(() -> new NoSuchElementException("No value present"));
      return transform(results, HostAndPort::host);

    } catch (RuntimeException e) {
      // NameNotFound is a checked exception, so reactor wraps it in a runtime exception.
      NameNotFoundException cause = CbThrowables.findCause(e, NameNotFoundException.class).orElse(null);
      if (cause != null) {
        throw cause;
      }
      throw e;
    }
  }
}
