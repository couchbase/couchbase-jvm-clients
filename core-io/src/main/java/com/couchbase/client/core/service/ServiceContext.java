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

package com.couchbase.client.core.service;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.util.HostAndPort;

import java.util.Map;
import java.util.Optional;

public class ServiceContext extends CoreContext {

  private final HostAndPort remote;

  /**
   * The service type of this context.
   */
  private final ServiceType serviceType;

  private final Optional<String> bucket;

  public ServiceContext(CoreContext ctx, String remoteHostname, int remotePort,
                        ServiceType serviceType, Optional<String> bucket) {
    super(ctx.core(), ctx.id(), ctx.environment(), ctx.authenticator());
    this.bucket = bucket;
    this.serviceType = serviceType;
    this.remote = new HostAndPort(remoteHostname, remotePort);
  }

  /**
   * @deprecated In favor of {@link #remote()}
   */
  @Deprecated
  public String remoteHostname() {
    return remote.host();
  }

  /**
   * @deprecated In favor of {@link #remote()}
   */
  @Deprecated
  public int remotePort() {
    return remote.port();
  }

  public HostAndPort remote() {
    return remote;
  }

  @Override
  public void injectExportableParams(final Map<String, Object> input) {
    super.injectExportableParams(input);
    input.put("remote", remote.toString());
    input.put("type", serviceType);
    bucket.ifPresent(b -> input.put("bucket", b));
  }

  public ServiceType serviceType() {
    return serviceType;
  }

  public Optional<String> bucket() {
    return bucket;
  }
}
