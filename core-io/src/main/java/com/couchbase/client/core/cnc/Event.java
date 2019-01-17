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
 * The parent interface for all events pushed through the command and
 * control system.
 */
public interface Event {

  /**
   * Contains a nano timestamp when the event was created.
   *
   * @return the time this event got created.
   */
  long createdAt();

  /**
   * The Severity of this event.
   *
   * @return the event severity.
   */
  Severity severity();

  /**
   * The Category of this event.
   *
   * @return the event category.
   */
  Category category();

  /**
   * Returns the duration of this event.
   *
   * @return the duration of the even, 0 if not set.
   */
  Duration duration();

  /**
   * The context this event is referencing.
   *
   * @return the referencing context.
   */
  Context context();

  /**
   * A textual description with more information about the event.
   *
   * @return the description, if set.
   */
  String description();

  /**
   * Describes the severity of any given event.
   */
  enum Severity {
    /**
     * Verbose information used to trace certain actual
     * data throughout the system.
     */
    VERBOSE,

    /**
     * Information that guide debugging and in-depth
     * troubleshooting.
     */
    DEBUG,

    /**
     * Should rely non-critical information.
     */
    INFO,

    /**
     * Indicates that a component is in a non-ideal state
     * and that something could escalate into an error
     * potentially.
     */
    WARN,

    /**
     * Critical errors that require immediate attention and/or
     * problems which are not recoverable by the system itself.
     */
    ERROR,

    /**
     * Events that are created which deal with request and response tracing
     * (not to be confused with TRACE logging which is on purpose called
     * VERBOSE here so that they are not easily confused).
     */
    TRACING
  }

  /**
   * Describes the category of any given event.
   */
  enum Category {
    /**
     * Represents an event from the IO subsystem.
     */
    IO,
    /**
     * Represents an event from the Endpoint layer.
     */
    ENDPOINT,
    /**
     * Represents an event around a specific request instance.
     */
    REQUEST,
    /**
     * Represents an event that concerns the JVM/OS/system.
     */
    SYSTEM,
    /**
     * Represents an event from the Service layer.
     */
    SERVICE,
    /**
     * Represents an event from the Node layer.
     */
    NODE,
    /**
     * Represents an event from the config subsystem.
     */
    CONFIG,
  }

}
