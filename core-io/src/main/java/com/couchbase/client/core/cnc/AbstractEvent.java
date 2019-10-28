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

package com.couchbase.client.core.cnc;

import java.time.Duration;

/**
 * This {@link Event} implementation can be used as a base event to inherit from.
 */
public abstract class AbstractEvent implements Event {

  private final Severity severity;
  private final String category;
  private final Duration duration;
  private final Context context;
  private final long createdAt;

  protected AbstractEvent(Severity severity, Category category, Duration duration, Context context) {
    this(severity, category.path(), duration, context);
  }

  /**
   * Creates a new abstract event.
   *
   * @param severity the severity to use.
   * @param category the category to use.
   * @param duration the duration for this event.
   * @param context the context if provided.
   */
  protected AbstractEvent(Severity severity, String category, Duration duration, Context context) {
    this.severity = severity;
    this.category = category;
    this.duration = duration;
    this.context = context;
    this.createdAt = System.nanoTime();
  }

  @Override
  public Severity severity() {
    return severity;
  }

  @Override
  public String category() {
    return category;
  }

  @Override
  public Duration duration() {
    return duration;
  }

  @Override
  public Context context() {
    return context;
  }

  @Override
  public long createdAt() {
    return createdAt;
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + "{"
      + "severity=" + severity
      + ", category=" + category
      + ", duration=" + duration
      + ", createdAt=" + createdAt
      + ", description=" + description()
      + ", context=" + context
      + ", cause=" + cause()
      + '}';
  }
}
