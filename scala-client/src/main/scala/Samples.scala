import com.couchbase.client.scala._

import scala.concurrent.duration._

object Samples {
  def main(args: Array[String]): Unit = {
    // Just for demoing, really this would come from cluster.openCollection("scope", "people") or similar
    val coll = new Collection()

    // Gets return Option[JsonDocument]
    val fetched1 = coll.get("id")

    // All parameters provided by named/default parameters rather than a GetOptions class.  Not wedded to
    // this, just feel it's more idiomatically Scala
    val fetched2 = coll.get("id", timeout = 1000.milliseconds)
    val feteched3 = coll.getAndLock("id", 5.seconds)

    // Basic insert
    val toInsert = JsonDocument.create("id", JsonObject.empty())
    val docPostInsert = coll.insert(toInsert)

    // Basic insert without creating a JsonDocument
    // One downside of named/default params is that I have to give this a different name than 'insert'
    coll.insertContent("id", JsonObject.empty())

    // Insert - showing all parameters
    coll.insert(toInsert,
      timeout = 1000.milliseconds,
      expiration = 10.days,
      replicateTo = ReplicateTo.ALL,
      persistTo = PersistTo.MAJORITY
    )

    // Insert, providing a subset of parameters
    coll.insert(toInsert, timeout = 1000.milliseconds, persistTo = PersistTo.MAJORITY)

    // Basic replaces.  JsonDocument is an immutable Scala case class and it's trivial to copy
    // it with different content:
    if (fetched1.isDefined) {
      val toReplace = fetched1.get.copy(content = JsonObject.empty())
      coll.replace(toReplace)

      // Replace, providing a subset of parameters
      coll.replace(toReplace, timeout = 1000.milliseconds, persistTo = PersistTo.MAJORITY)
    }
  }
}