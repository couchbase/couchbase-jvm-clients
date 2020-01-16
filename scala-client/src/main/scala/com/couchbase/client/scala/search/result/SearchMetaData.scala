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
package com.couchbase.client.scala.search.result

/** Additional information returned by the FTS service after any rows and errors.
  *
  * @param metrics         metrics related to the FTS request, if they are available
  * @param errors         any errors returned by the request.  Note that FTS can return partial success: e.g. some
  *                       rows in the presence of some errors
  */
case class SearchMetaData(metrics: SearchMetrics, errors: collection.Map[String, String])
