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

package com.couchbase.client.core.msg.manager;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.msg.BaseResponse;
import com.couchbase.client.core.msg.ResponseStatus;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

public class BucketConfigStreamingResponse extends BaseResponse {

  private final Sinks.Many<String> configsSink = Sinks.many().replay().latest();
  private final Flux<String> configs = configsSink.asFlux();
  private final String address;

  BucketConfigStreamingResponse(final ResponseStatus status, final String address) {
    super(status);
    this.address = address;
  }

  @Stability.Internal
  public void pushConfig(final String config) {
    configsSink.tryEmitNext(config);
  }

  @Stability.Internal
  public void completeStream() {
    configsSink.tryEmitComplete();
  }

  @Stability.Internal
  public void failStream(final Throwable e) {
    configsSink.tryEmitError(e);
  }

  public Flux<String> configs() {
    return configs;
  }

  public String address() {
    return address;
  }

}
