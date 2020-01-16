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
package test.app
import com.couchbase.client.scala.Collection
import com.couchbase.client.scala.durability.Durability
import com.couchbase.client.scala.json.JsonObject
import com.couchbase.client.scala.kv.{InsertOptions, ReplaceOptions}
import org.junit.jupiter.api.Test

/**
  * There's a lot of private[scala] code, and most tests are under the .scala namespace too.  This file just checks
  * that a regular app can reach everything needed.
  */
class CompileTest {
  def collection(): Unit = {
    val coll: Collection = null

    coll.insert("id", JsonObject.create, durability = Durability.Disabled)
    coll.insert("id", JsonObject.create, InsertOptions(durability = Durability.Disabled))
    coll.insert("id", JsonObject.create, InsertOptions().durability(Durability.Disabled))

    coll.replace("id", JsonObject.create, durability = Durability.Disabled)
    coll.replace("id", JsonObject.create, ReplaceOptions(durability = Durability.Disabled))
    coll.replace("id", JsonObject.create, ReplaceOptions().durability(Durability.Disabled))
  }
}
