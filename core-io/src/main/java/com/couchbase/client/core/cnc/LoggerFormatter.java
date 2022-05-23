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
package com.couchbase.client.core.cnc;

import reactor.util.annotation.Nullable;

import java.util.logging.Level;

/**
 * Interface allowing to customize the output format of the logger (right now only used for console logging).
 */
public interface LoggerFormatter {

  /**
   * Formats the inputs into a string that will be printed.
   *
   * @param logLevel the log level.
   * @param message the message.
   * @param throwable the throwable, which can be null.
   *
   * @return the formatted string that will be printed onto the console.
   */
  String format(Level logLevel, String message, @Nullable Throwable throwable);

}
