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

package com.couchbase.client.core.cnc.events.core;

import com.couchbase.client.core.cnc.AbstractEvent;

import java.time.Duration;
import java.util.List;

import static com.couchbase.client.core.logging.RedactableArgument.redactSystem;

/**
 * Raised when the SDK could properly load hostnames from a DNS SRV record.
 */
public class DnsSrvRecordsLoadedEvent extends AbstractEvent {

  private final List<String> seeds;

  public DnsSrvRecordsLoadedEvent(final Duration duration, final List<String> seeds) {
    super(Severity.INFO, Category.CORE, duration, null);
    this.seeds = seeds;
  }

  public List<String> seeds() {
    return seeds;
  }

  @Override
  public String description() {
    return "Loaded seed hosts from DNS SRV: " + redactSystem(seeds.toString());
  }

}
