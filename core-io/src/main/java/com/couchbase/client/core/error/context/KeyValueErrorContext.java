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

package com.couchbase.client.core.error.context;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.msg.kv.KeyValueBaseResponse;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.kv.KeyValueRequest;
import reactor.util.annotation.Nullable;

import java.util.Map;

@Stability.Uncommitted
public class KeyValueErrorContext extends ErrorContext {

  private final KeyValueRequest<?> request;
  private final @Nullable MemcacheProtocol.FlexibleExtras flexibleExtras;

  protected KeyValueErrorContext(final KeyValueRequest<?> request, final ResponseStatus status, @Nullable MemcacheProtocol.FlexibleExtras flexibleExtras) {
    super(status);
    this.request = request;
    this.flexibleExtras = flexibleExtras;
  }

  public static KeyValueErrorContext completedRequest(final KeyValueRequest<?> request,
                                                      final Response response) {
    MemcacheProtocol.FlexibleExtras flexibleExtras = null;
    if (response instanceof KeyValueBaseResponse) {
      flexibleExtras = ((KeyValueBaseResponse) response).flexibleExtras();
    }
    return new KeyValueErrorContext(request, response.status(), flexibleExtras);
  }

  public static KeyValueErrorContext completedRequest(final KeyValueRequest<?> request,
                                                      final ResponseStatus status,
                                                      @Nullable MemcacheProtocol.FlexibleExtras flexibleExtras) {
    return new KeyValueErrorContext(request, status, flexibleExtras);
  }

  public static KeyValueErrorContext incompleteRequest(final KeyValueRequest<?> request) {
    return new KeyValueErrorContext(request, null, null);
  }

  @Override
  public void injectExportableParams(final Map<String, Object> input) {
    super.injectExportableParams(input);
    if (request != null) {
      request.context().injectExportableParams(input);
    }
    if (flexibleExtras != null) {
      flexibleExtras.injectExportableParams(input);
    }
  }

}
