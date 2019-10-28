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

package com.couchbase.client.core.cnc.events.config;

import com.couchbase.client.core.cnc.AbstractEvent;
import com.couchbase.client.core.cnc.Context;

import java.time.Duration;
import java.util.Optional;

public class ConfigIgnoredEvent extends AbstractEvent {

  private final Optional<String> config;
  private final Reason reason;
  private final Optional<Exception> cause;

  public ConfigIgnoredEvent(final Context context, final Reason reason,
                            final Optional<Exception> cause, final Optional<String> config) {
    super(reason.severity(), Category.CONFIG, Duration.ZERO, context);
    this.reason = reason;
    this.cause = cause;
    this.config = config;
  }

  public Reason reason() {
    return reason;
  }

  @Override
  public Exception cause() {
    return cause.orElse(null);
  }

  public Optional<String> config() {
    return config;
  }

  @Override
  public String description() {
    String msg = "The proposed configuration was ignored because of: " + reason();
    if (reason == Reason.PARSE_FAILURE && config.isPresent()) {
      msg += "; Config: " + config.get();
    }
    if (cause.isPresent()) {
      msg += "; Cause: " + cause.get().getMessage();
    }
    return msg;
  }

  /**
   * The reason why the proposed config got ignored.
   */
  public enum Reason {
    /**
     * The proposed config has the same or an older revision number.
     */
    OLD_OR_SAME_REVISION(Severity.VERBOSE),
    /**
     * The provider is already shutdown when the config was provided.
     */
    ALREADY_SHUTDOWN(Severity.VERBOSE),
    /**
     * The config could not be parsed. see cause for more information.
     */
    PARSE_FAILURE(Severity.WARN);

    private final Severity severity;

    Reason(Severity severity) {
      this.severity = severity;
    }

    public Severity severity() {
      return severity;
    }
  }

}
