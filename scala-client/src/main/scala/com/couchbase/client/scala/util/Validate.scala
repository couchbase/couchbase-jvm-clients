package com.couchbase.client.scala.util

import scala.util.{Failure, Success, Try}

object Validate {
  def optNotNull(input: Option[Any], identifier: String): Try[Any] = {
    if (input == null) Failure(new IllegalArgumentException(identifier + " cannot be null"))
    if (input.isDefined && input.get == null) Failure(new IllegalArgumentException(identifier + " cannot be null"))
    else Success(input)
  }


  def notNull(input: Any, identifier: String): Try[Any] = {
    if (input == null) Failure(new IllegalArgumentException(identifier + " cannot be null"))
    else Success(input)
  }

  /**
    * Check if the given string is not null or empty.
    *
    * <p>If it is null or empty, a `Failure(IllegalArgumentException)` is raised with a
    * proper message.</p>
    *
    * @param input      the string to check.
    * @param identifier the identifier that is part of the exception message.
    */
  def notNullOrEmpty(input: String, identifier: String): Try[String] = {
    if (input == null || input.isEmpty) Failure(new IllegalArgumentException(identifier + " cannot be null or empty"))
    else Success(input)
  }

  def notNullOrEmpty(input: Seq[_], identifier: String): Try[Seq[_]] = {
    if (input == null || input.isEmpty) Failure(new IllegalArgumentException(identifier + " cannot be null or empty"))
    else Success(input)
  }
}
