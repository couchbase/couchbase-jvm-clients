/*
 * Copyright 2023 Couchbase, Inc.
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

package com.couchbase.client.core.util;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.org.xbill.DNS.ExtendedResolver;
import com.couchbase.client.core.deps.org.xbill.DNS.Name;
import com.couchbase.client.core.deps.org.xbill.DNS.ResolverConfig;
import com.couchbase.client.core.deps.org.xbill.DNS.SRVRecord;
import com.couchbase.client.core.deps.org.xbill.DNS.TextParseException;
import com.couchbase.client.core.deps.org.xbill.DNS.Type;
import com.couchbase.client.core.deps.org.xbill.DNS.lookup.LookupSession;
import com.couchbase.client.core.deps.org.xbill.DNS.lookup.NoSuchDomainException;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import javax.naming.NameNotFoundException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.Executor;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

@Stability.Internal
public class DnsSrvResolver {

  private final Executor executor;
  @Nullable private final String nameserver;

  public DnsSrvResolver(Executor executor) {
    this(executor, null);
  }

  DnsSrvResolver(Executor executor, @Nullable String nameserver) {
    this.executor = requireNonNull(executor);
    this.nameserver = nameserver;

    // fail fast if nameserver address is invalid
    if (nameserver != null) {
      try {
        //noinspection ResultOfMethodCallIgnored
        InetAddress.getByName(nameserver);
      } catch (UnknownHostException e) {
        throw new IllegalArgumentException("Invalid nameserver IP address: " + nameserver, e);
      }
    }
  }

  public Mono<List<HostAndPort>> resolve(String name) {
    Name parsedName = parseName(name);

    return Mono.defer(() -> {
      // Create a new session each time in case the resolver config changes
      // (different server list, search path, etc.)
      // See https://stackoverflow.com/questions/42444575/android-dns-java-srv-lookup-fails-when-network-connectivity-changes
      LookupSession session = newLookupSession();

      return Mono.fromFuture(
          session
            .lookupAsync(parsedName, Type.SRV)
            .toCompletableFuture()
        )
        .onErrorResume(t ->
          t instanceof NoSuchDomainException
            ? Mono.error(new NameNotFoundException(t.toString()))
            : Mono.error(t)
        )
        .map(lookupResult ->
          lookupResult.getRecords().stream()
            .map(record -> (SRVRecord) record)
            .map(record -> new HostAndPort(
              record.getTarget().toString(true),
              record.getPort()
            ))
            .collect(toList())
        ).flatMap(it ->
          it.isEmpty()
            ? Mono.error(new NameNotFoundException("DNS SRV lookup was apparently successful, but returned no records."))
            : Mono.just(it)
        );
    });
  }

  private static Name parseName(String name) {
    try {
      return Name.fromString(name);
    } catch (TextParseException e) {
      throw new IllegalArgumentException(e);
    }
  }

  private LookupSession newLookupSession() {
    try {
      synchronized (ResolverConfig.class) {
        ResolverConfig.refresh();
        return LookupSession.defaultBuilder()
          .resolver(this.nameserver == null ? new ExtendedResolver() : new ExtendedResolver(new String[]{nameserver}))
          .searchPath(ResolverConfig.getCurrentConfig().searchPath())
          .executor(executor)
          .build();
      }
    } catch (UnknownHostException e) {
      // Shouldn't happen, since we checked in the constructor. Just in case:
      throw new IllegalArgumentException("Bad nameserver IP address: " + nameserver, e);
    }
  }
}
