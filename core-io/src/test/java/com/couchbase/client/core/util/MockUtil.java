/*
 * Copyright (c) 2024 Couchbase, Inc.
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

import com.couchbase.client.core.Core;
import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.CoreResources;
import com.couchbase.client.core.cnc.RequestTracer;
import com.couchbase.client.core.cnc.apptelemetry.collector.AppTelemetryCollector;
import com.couchbase.client.core.cnc.tracing.NoopRequestTracer;
import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.PasswordAuthenticator;

import java.util.function.Function;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MockUtil {
  private MockUtil() {
  }

  /**
   * Returns a mock core with a mock {@link CoreEnvironment} and mock {@link CoreContext}.
   */
  public static Core mockCore() {
    return mockCore(mock(CoreEnvironment.class));
  }

  /**
   * Returns a mock core with the given {@link CoreEnvironment} and mock {@link CoreContext}.
   */
  public static Core mockCore(CoreEnvironment env) {
    return mockCore(
      env,
      core -> {
        CoreContext ctx = mock(CoreContext.class);
        Authenticator authenticator = PasswordAuthenticator.create("username", "password");
        CoreEnvironment env1 = core.environment();
        CoreResources resources = core.coreResources();

        when(ctx.core()).thenReturn(core);
        when(ctx.coreResources()).thenReturn(resources);
        when(ctx.id()).thenReturn(1L);
        when(ctx.environment()).thenReturn(env1);
        when(ctx.authenticator()).thenReturn(authenticator);
        return ctx;
      }
    );
  }

  /**
   * Returns a mock core with the given {@link CoreEnvironment} and a mock {@link CoreContext}
   * that uses the given username and password.
   */
  public static Core mockCore(
    CoreEnvironment env,
    String username,
    String password
  ) {
    Core core = mockCore(env);
    CoreContext ctx = core.context();
    when(ctx.authenticator()).thenReturn(PasswordAuthenticator.create(username, password));
    return core;
  }

  /**
   * Returns a mock core with the given {@link CoreEnvironment}
   * and a {@link CoreContext} returned by the given factory function.
   */
  private static Core mockCore(
    CoreEnvironment env,
    Function<Core, CoreContext> coreContextFactory
  ) {
    CoreResources coreResources = new CoreResources() {
      @Override
      public RequestTracer requestTracer() {
        return NoopRequestTracer.INSTANCE;
      }
    };

    Core core = mock(Core.class);
    when(core.coreResources()).thenReturn(coreResources);
    when(core.appTelemetryCollector()).thenReturn(AppTelemetryCollector.NOOP);
    when(core.environment()).thenReturn(env);

    CoreContext ctx = coreContextFactory.apply(core);
    when(core.context()).thenReturn(ctx);

    return core;
  }

  /**
   * Returns a mock subclass of CoreContext linked to the given core.
   * For mocking EndpointContext, ServiceContext, etc.
   */
  public static <T extends CoreContext> T mockCoreContext(Core core, Class<T> contextClass) {
    T ctx = mock(contextClass);
    copyCoreContextProperties(core.context(), ctx);
    return ctx;
  }

  private static void copyCoreContextProperties(CoreContext source, CoreContext dest) {
    Core core = source.core();
    CoreResources resources = source.coreResources();
    long id = source.id();
    CoreEnvironment env = source.environment();
    Authenticator authenticator = source.authenticator();

    when(dest.core()).thenReturn(core);
    when(dest.coreResources()).thenReturn(resources);
    when(dest.id()).thenReturn(id);
    when(dest.environment()).thenReturn(env);
    when(dest.authenticator()).thenReturn(authenticator);
  }
}
