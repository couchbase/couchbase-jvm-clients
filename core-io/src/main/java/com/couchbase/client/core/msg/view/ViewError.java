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

package com.couchbase.client.core.msg.view;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.json.Mapper;

import java.util.HashMap;
import java.util.Map;

public class ViewError {

  private String error;
  private String reason;

  public ViewError(String error, String reason) {
    this.error = error;
    this.reason = reason;
  }

  public String error() {
    return error;
  }

  public String reason() {
    return reason;
  }

  @Stability.Internal
  public String reassemble() {
    Map<String, String> converted = new HashMap<>(2);
    converted.put("error", error);
    converted.put("reason", reason);
    return Mapper.encodeAsString(converted);
  }

}
