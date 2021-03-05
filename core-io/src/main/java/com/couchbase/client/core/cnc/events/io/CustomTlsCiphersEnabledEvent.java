/*
 * Copyright (c) 2021 Couchbase, Inc.
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
package com.couchbase.client.core.cnc.events.io;

import com.couchbase.client.core.cnc.AbstractEvent;
import com.couchbase.client.core.cnc.Context;

import java.time.Duration;
import java.util.List;

/**
 * This event is emitted if the user has configured a custom list of tls ciphers.
 */
public class CustomTlsCiphersEnabledEvent extends AbstractEvent {

  private final List<String> ciphers;

  public CustomTlsCiphersEnabledEvent(List<String> ciphers, Context context) {
    super(Severity.DEBUG, Category.IO, Duration.ZERO, context);
    this.ciphers = ciphers;
  }

  public List<String> ciphers() {
    return ciphers;
  }

  @Override
  public String description() {
    return "A custom list of ciphers has been selected: " + ciphers;
  }

}
