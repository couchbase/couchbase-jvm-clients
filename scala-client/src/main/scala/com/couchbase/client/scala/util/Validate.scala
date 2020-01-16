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
package com.couchbase.client.scala.util

import scala.util.{Failure, Success, Try}

/** Utility functions to validate arguments.
  *
  * @author Graham Pople
  * @since 1.0.0
  */
private[scala] object Validate {
  def optNotNull(input: Option[Any], identifier: String): Try[Any] = {
    if (input == null) Failure(new IllegalArgumentException(identifier + " cannot be null"))
    if (input.isDefined && input.get == null)
      Failure(new IllegalArgumentException(identifier + " cannot be null"))
    else Success(input)
  }

  def notNull(input: Any, identifier: String): Try[Any] = {
    if (input == null) Failure(new IllegalArgumentException(identifier + " cannot be null"))
    else Success(input)
  }

  /** Check if the given string is not null or empty.
    *
    * <p>If it is null or empty, a `Failure(IllegalArgumentException)` is raised with a
    * proper message.</p>
    *
    * @param input      the string to check.
    * @param identifier the identifier that is part of the exception message.
    */
  def notNullOrEmpty(input: String, identifier: String): Try[String] = {
    if (input == null || input.isEmpty)
      Failure(new IllegalArgumentException(identifier + " cannot be null or empty"))
    else Success(input)
  }

  def notNullOrEmpty(input: collection.Seq[_], identifier: String): Try[collection.Seq[_]] = {
    if (input == null || input.isEmpty)
      Failure(new IllegalArgumentException(identifier + " cannot be null or empty"))
    else Success(input)
  }
}
