/*
 * Copyright (c) 2022 Couchbase, Inc.
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

package com.couchbase.client.core.msg.kv;

import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.msg.BaseResponse;
import com.couchbase.client.core.msg.ResponseStatus;
import reactor.util.annotation.Nullable;

/**
 * The parent class for all KV responses passing through the SDK that potentially contain flexible extras.
 *
 * @since 2.3.4
 */
public class KeyValueBaseResponse extends BaseResponse {

  private final @Nullable MemcacheProtocol.FlexibleExtras flexibleExtras;

  protected KeyValueBaseResponse(ResponseStatus status) {
    super(status);
    this.flexibleExtras = null;
  }

  protected KeyValueBaseResponse(ResponseStatus status, @Nullable MemcacheProtocol.FlexibleExtras flexibleExtras) {
    super(status);
    this.flexibleExtras = flexibleExtras;
  }

  @Nullable
  public MemcacheProtocol.FlexibleExtras flexibleExtras() {
    return flexibleExtras;
  }
}
