import java.time.Duration
import java.time.temporal.ChronoUnit

import com.couchbase.client.core.error._
import com.couchbase.client.scala.CouchbaseCluster
import com.couchbase.client.scala.api._
import com.couchbase.client.scala.document.{Document, JsonArray, JsonObject}
import reactor.retry.{Jitter, Retry, RetryContext}

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext

class Scenarios {
  private implicit val ec = ExecutionContext.Implicits.global

  private val cluster = CouchbaseCluster.create("localhost")
  private val bucket = cluster.openBucket("default")
  private val scope = bucket.openScope("scope")
  private val coll = scope.openCollection("people")


  def scenarioA(): Unit = {
    val doc: Document = coll.getOrError("id", GetOptions().timeout(10.seconds))

    val content: JsonObject = doc.content.put("field", "value")

    val result: MutationResult = coll.replace(doc.id, content, doc.cas, ReplaceOptions().timeout(10.seconds))


    // Default params also supported for all methods
    val doc2: Document = coll.getOrError("id", timeout = 10.seconds)

    val content2: JsonObject = doc2.content
      .put("field", "value")
      .put("foo", "bar")

    coll.replace(doc2.id, content2, doc2.cas, timeout = 10.seconds)


    // I include type annotations and getOrError above to make things clearer, but it'd be more idiomatic to write this:
    coll.get("id", timeout = 10.seconds) match {
      case Some(doc3) =>
        coll.replace(doc3.id,
          doc3.content
            .put("field", "value")
            .put("foo", "bar"),
          doc3.cas,
          timeout = 10.seconds)

      case _ => println("could not get doc")
    }
  }


  def scenarioB(): Unit = {
    val subdocOpt: Option[SubDocument] = coll.lookupIn("id", LookupInSpec().get("someArray"), timeout = 10.seconds)

    subdocOpt.map(subdoc => {
      val arr: JsonArray = subdoc.contentAsArray
      arr.add("foo")

      val result: MutationResult = coll.mutateIn("id", MutateInSpec().upsert("someArray", arr), MutateInOptions().timeout(10.seconds))
    })
  }



  def scenarioC_clientSideDurability(): Unit = {
    // Use a helper wrapper to retry our operation in the face of durability failures
    // remove is idempotent iff the app guarantees that the doc's id won't be reused (e.g. if it's a UUID).  This seems
    // a reasonable restriction.
    retryIdempotentRemoveClientSide((replicateTo: ReplicateTo.Value) => {
      val result: MutationResult = coll.remove("id", cas = 0, RemoveOptions().durabilityClient(replicateTo, PersistTo.None))
    }, ReplicateTo.Two, ReplicateTo.Two, System.nanoTime().nanos.plus(30.seconds))
  }

  /**
    * Automatically retries an idempotent operation in the face of durability failures
    * TODO this is quite complex logic.  Should this be folded into the client as a per-operation retry strategy?
    * @param callback an idempotent remove operation to perform
    * @param replicateTo the current ReplicateTo setting being tried
    * @param originalReplicateTo the originally requested ReplicateTo setting
    * @param until prevent the operation looping indefinitely
    */
  private def retryIdempotentRemoveClientSide(callback: (ReplicateTo.Value) => Unit, replicateTo: ReplicateTo.Value, originalReplicateTo: ReplicateTo.Value, until: FiniteDuration): Unit = {
    if (System.nanoTime().nanos >= until) {
      // Depending on the durability requirements, may want to also log this to an external system for human review
      // and reconciliation
      throw new RuntimeException("Failed to durably write operation")
    }

    try {
      callback(replicateTo)
    }
    catch {
      case err: DocumentDoesNotExistException =>
        println("Our work here is done")

      case err: ReplicaNotConfiguredException =>
        println("Not enough replicas configured, aborting")

      case err: DocumentConcurrentlyModifiedException =>
        // Just retry
        retryIdempotentRemoveClientSide(callback, replicateTo, originalReplicateTo, until)

      case err: DocumentMutationLostException =>
        // Mutation lost during a hard failover.  I *think* we just retry with the original replicateTo.  If enough replicas
        // still aren't available, it will presumably raise ReplicaNotAvailableException and retry with lower.
        retryIdempotentRemoveClientSide(callback, originalReplicateTo, originalReplicateTo, until)

      case err: ReplicaNotAvailableException =>
        val newReplicateTo = replicateTo match {
          case ReplicateTo.One => ReplicateTo.None
          case ReplicateTo.Two => ReplicateTo.One
          case ReplicateTo.Three => ReplicateTo.Two
          case _ => ReplicateTo.None
        }
        println("Temporary replica failure, retrying with lower durability " + newReplicateTo)
        retryIdempotentRemoveClientSide(callback, newReplicateTo, originalReplicateTo, until)
    }
  }


  def scenarioC_serverSideDurability(): Unit = {
    // Use a helper wrapper to retry our operation in the face of durability failures
    // remove is idempotent iff the app guarantees that the doc's id won't be reused (e.g. if it's a UUID).  This seems
    // a reasonable restriction.
    retryIdempotentRemoveServerSide(() => {
      coll.remove("id", cas = 0, RemoveOptions().durabilityServer(Durability.MajorityAndPersistActive))
    }, System.nanoTime().nanos.plus(30.seconds))
  }

  /**
    * Automatically retries an idempotent operation in the face of durability failures
    * TODO Should this be folded into the client as a per-operation retry strategy?
    * @param callback an idempotent remove operation to perform
    * @param until prevent the operation looping indefinitely
    */
  private def retryIdempotentRemoveServerSide(callback: () => Unit, until: FiniteDuration): Unit = {
    if (System.nanoTime().nanos >= until) {
      // Depending on the durability requirements, may want to also log this to an external system for human review
      // and reconciliation
      throw new RuntimeException("Failed to durably write operation")
    }

    try {
      callback()
    }
    catch {
      // Not entirely clear what failures need to be handled yet, but will be considerably easier than observe() based
      // logic.  I think the only case to handle is:

      case err: DurabilityAmbiguous =>
        // A guarantee is that the mutation is either written to a majority of nodes, or none.  But we don't know which.
        coll.get("id") match {
          case Some(doc) => retryIdempotentRemoveServerSide(callback, until)
          case _ => println("Our work here is done")
        }

      case err: DocumentDoesNotExistException =>
        println("Our work here is done")
    }
  }


  def scenarioD(): Unit = {
    retryOperationOnCASMismatch(() => {
      coll.get("id", timeout = 10.seconds) match {
        case Some(doc) =>
          coll.replace(doc.id,
            doc.content
              .put("field", "value")
              .put("foo", "bar"),
            doc.cas,
            timeout = 10.seconds)

        case _ => println("could not get doc")
      }
    }, guard = 50)
  }

  private def retryOperationOnCASMismatch(callback: () => Unit, guard: Int): Unit = {
    if (guard <= 0) {
      throw new RuntimeException("Failed to perform exception")
    }

    try {
      callback()
    }
    catch {
      case err: CASMismatchException => retryOperationOnCASMismatch(callback, guard - 1)
    }
  }


  def scenarioE(): Unit = {
    case class User(name: String, age: Int, address: String, phoneNumber: String)

    coll.get("id", timeout = 10.seconds) match {
      case Some(doc) =>
        val user: User = doc.contentAs[User]
        val changed = user.copy(age = 25)
        coll.replace(doc.id, changed, doc.cas, timeout = 10.seconds)

      case _ => println("could not get doc")
    }
  }


  // {
  //   user: {
  //     name = "bob",
  //     age = 23,
  //     address = "123 Fake Street"
  //   }
  // }

  def scenarioF_fulldoc(): Unit = {
    case class User(name: String, age: Int, address: String)

    coll.get("id") match {
      case Some(doc) =>
        val user: User = doc.contentAs[User]
        val changed = user.copy(age = 25)

        // Is this too dangerous?  What if a newer client has added 'phoneNumber' to user that this older client doesn't know about?
        coll.replace(doc.id, changed, doc.cas)

      case _ => println("could not find doc")
    }
  }


  def scenarioF_subdoc(): Unit = {
    case class UserPartial(name: String, age: Int)

    val subdoc: SubDocument = coll.lookupIn("id", LookupInSpec().get("user.name", "user.age")).get

    val user: UserPartial = subdoc.contentAs[UserPartial]
    val changed = user.copy(age = 25)

    // mergeUpsert will upsert fields user.name & user.age, leaving user.address alone
    coll.mutateIn(subdoc.id, MutateInSpec().mergeUpsert("user", changed))
  }
}
