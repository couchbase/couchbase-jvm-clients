/*
 * Copyright (c) 2023 Couchbase, Inc.
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
package com.couchbase.client.core.endpoint;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.io.grpc.Attributes;
import com.couchbase.client.core.deps.io.grpc.EquivalentAddressGroup;
import com.couchbase.client.core.deps.io.grpc.NameResolver;

import java.net.URI;
import java.util.List;

// Credit: https://sultanov.dev/blog/grpc-client-side-load-balancing/
// JVMCBC-1192: Only being used for performance testing and will be removed pre-GA.
@Stability.Internal
public class MultiAddressNameResolverFactory extends NameResolver.Factory {

  final List<EquivalentAddressGroup> addresses;

  MultiAddressNameResolverFactory(List<EquivalentAddressGroup> addresses) {
    this.addresses = addresses;
  }

  public NameResolver newNameResolver(URI notUsedUri, NameResolver.Args args) {
    return new NameResolver() {
      @Override
      public String getServiceAuthority() {
        return "fakeAuthority";
      }

      public void start(Listener2 listener) {
        listener.onResult(ResolutionResult.newBuilder().setAddresses(addresses).setAttributes(Attributes.EMPTY).build());
      }

      public void shutdown() {
      }
    };
  }

  @Override
  public String getDefaultScheme() {
    return "multiaddress";
  }
}
