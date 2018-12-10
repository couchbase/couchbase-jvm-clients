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

package com.couchbase.client.test;

import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolver;

/**
 * This invocation provider starts and stops the couchbase cluster before and after
 * all tests are executed.
 *
 * <p>Note that the internals on what and how the containers are managed is up to the
 * {@link TestCluster} implementation, and if it is unmanaged than this
 * very well be mostly a "stub".</p>
 *
 * @since 2.0.0
 */
public class ClusterInvocationProvider implements BeforeAllCallback, ParameterResolver {

  /**
   * Identifier for the container in the root store.
   */
  private static final String STORE_KEY = "db";

  /**
   * The cluster container, once set.
   */
  private volatile TestCluster testCluster;

  @Override
  public void beforeAll(final ExtensionContext ctx) {
    rootStore(ctx).getOrComputeIfAbsent(STORE_KEY, key -> {
      testCluster = TestCluster.create();
      testCluster.start();
      return testCluster;
    });
  }

  /**
   * Grabs the root store from the global namespace in junit.
   *
   * @param ctx the extension context from the root store.
   * @return the loaded store.
   */
  private ExtensionContext.Store rootStore(final ExtensionContext ctx) {
    return ctx.getParent().get().getStore(ExtensionContext.Namespace.GLOBAL);
  }

  @Override
  public boolean supportsParameter(final ParameterContext pCtx, final ExtensionContext eCtx) {
    return pCtx.getParameter().getType().isAssignableFrom(TestClusterConfig.class);
  }

  @Override
  public Object resolveParameter(final ParameterContext pCtx, final ExtensionContext eCtx) {
    return ((TestCluster) rootStore(eCtx).get(STORE_KEY)).config();
  }

}
