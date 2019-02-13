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

package com.couchbase.client.scala.kv

import scala.language.dynamics

sealed trait LookupInSpec
private case class Get(path: String, xattr: Boolean) extends LookupInSpec
private case class GetFullDocument() extends LookupInSpec
private case class Exists(path: String, xattr: Boolean) extends LookupInSpec
private case class Count(path: String, xattr: Boolean) extends LookupInSpec


object LookupInSpec {
  def getDoc: LookupInSpec = {
    GetFullDocument()
  }

  def get(path: String, xattr: Boolean = false): LookupInSpec = {
    Get(path, xattr)
  }

  def count(path: String, xattr: Boolean = false): LookupInSpec = {
    Count(path, xattr)
  }

  def exists(path: String, xattr: Boolean = false): LookupInSpec = {
    Exists(path, xattr)
  }
}
