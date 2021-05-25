/*
 * Copyright (c) 2020 Couchbase, Inc.
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

import com.couchbase.client.core.msg.BaseResponse;
import com.couchbase.client.core.msg.ResponseStatus;

import java.util.Map;

/**
 * Response for the special purpose {@link MultiObserveViaCasRequest}.
 */
public class MultiObserveViaCasResponse extends BaseResponse {

  private final MultiObserveViaCasRequest request;
  private final Map<byte[], ObserveViaCasResponse.ObserveStatus> observed;

  public MultiObserveViaCasResponse(final MultiObserveViaCasRequest request, final ResponseStatus status,
                                    final Map<byte[], ObserveViaCasResponse.ObserveStatus> observed) {
    super(status);
    this.observed = observed;
    this.request = request;
  }

  public MultiObserveViaCasRequest request() {
    return request;
  }

  public Map<byte[], ObserveViaCasResponse.ObserveStatus> observed() {
    return observed;
  }

  @Override
  public String toString() {
    return "MultiObserveViaCasResponse{" +
      "status=" + status() +
      ", observed=" + observed +
      '}';
  }

}
