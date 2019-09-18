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

import com.couchbase.client.core.cnc.Context;
import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.env.CoreEnvironment;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

/**
 * Verifies the functionality of the {@link CoreContext}.
 */
class CoreContextTest {

  @Test
  void getAndExportProperties() {
    long id = 12345;
    CoreEnvironment env = mock(CoreEnvironment.class);
    Core core = mock(Core.class);
    Authenticator authenticator = mock(Authenticator.class);
    CoreContext ctx = new CoreContext(core, id, env, authenticator);

    assertEquals(core, ctx.core());
    assertEquals(id, ctx.id());
    assertEquals(env, ctx.environment());
    assertEquals(authenticator, ctx.authenticator());

    String result = ctx.exportAsString(Context.ExportFormat.JSON);
    assertEquals("{\"coreId\":12345}", result);
  }

  @Test
  void doesNotAllowNullAlternateIdentifier() {
    CoreContext ctx = new CoreContext(mock(Core.class), 1, mock(CoreEnvironment.class), mock(Authenticator.class));
    assertEquals(Optional.empty(), ctx.alternateAddress());
    assertThrows(IllegalArgumentException.class, () -> ctx.alternateAddress(null));
  }

}