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
package com.couchbase.client.core.cnc.events.config;

import com.couchbase.client.core.cnc.AbstractEvent;
import com.couchbase.client.core.cnc.Context;

import java.time.Duration;
import java.util.List;

/**
 * This event is raised if a DNS SRV refresh attempt completed successfully.
 */
public class DnsSrvRefreshAttemptCompletedEvent extends AbstractEvent {

  private final List<String> newSeeds;

  public DnsSrvRefreshAttemptCompletedEvent(final Duration duration, final Context context,
                                            final List<String> newSeeds) {
    super(Severity.INFO, Category.CONFIG, duration, context);
    this.newSeeds = newSeeds;
  }

  @Override
  public String description() {
    return "DNS SRV Refresh attempt completed successfully. New Seeds: " + newSeeds;
  }

}
