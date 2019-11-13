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

package com.couchbase.client.core;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.AbstractContext;
import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.env.CoreEnvironment;

import java.util.Map;
import java.util.Optional;

import static com.couchbase.client.core.util.Validators.notNull;

/**
 * The {@link CoreContext} is bound to a core and provides both exportable and
 * usable state for the rest of the application to use.
 *
 * @since 2.0.0
 */
public class CoreContext extends AbstractContext {

  /**
   * A (app local) unique ID per core instance.
   */
  private final long id;

  /**
   * The attached environment for this core.
   */
  private final CoreEnvironment env;

  /**
   * Back reference to the core itself.
   */
  private final Core core;

  /**
   * The authenticator to be used for this core.
   */
  private final Authenticator authenticator;

  /**
   * If present, contains the alternate address identifier that is used.
   */
  private volatile Optional<String> alternateAddress = Optional.empty();

  /**
  * Creates a new {@link CoreContext}.
  *
  * @param id the core id.
  * @param env the core environment.
  */
  public CoreContext(final Core core, final long id, final CoreEnvironment env, final Authenticator authenticator) {
    this.id = id;
    this.env = env;
    this.core = core;
    this.authenticator = authenticator;
  }

  /**
   * A (app local) unique ID per core instance.
   */
  public long id() {
    return id;
  }

  /**
   * The attached environment for this core.
   */
  public CoreEnvironment environment() {
    return env;
  }

  /**
   * Returns the alternate address identifier, if present.
   */
  public Optional<String> alternateAddress() {
    return alternateAddress;
  }

  /**
   * Returns the authenticator used for this core.
   */
  public Authenticator authenticator() {
    return authenticator;
  }

  /**
   * Sets the alternate address on this context.
   *
   * <p>This is internal API and will alter the behavior of the system. Do not call this API if you
   * are not 100% sure what you are doing!</p>
   *
   * @param alternateAddress the alternate address identifier, or empty if none available.
   * @return the same {@link CoreContext} for chaining purposes.
   */
  @Stability.Internal
  public CoreContext alternateAddress(final Optional<String> alternateAddress) {
    notNull(alternateAddress, "Alternate Address Identifier");
    this.alternateAddress = alternateAddress;
    return this;
  }

  /**
   * Returns the core to which this context belongs.
   */
  public Core core() {
    return core;
  }

  @Override
  public void injectExportableParams(final Map<String, Object> input) {
    input.put("coreId", id);
    alternateAddress.ifPresent(a -> input.put("alternateIdentifier", a));
  }

}
