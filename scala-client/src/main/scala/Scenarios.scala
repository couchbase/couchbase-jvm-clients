import java.time.Duration
import java.time.temporal.ChronoUnit

import com.couchbase.client.core.error._
import com.couchbase.client.scala.{Cluster}
import com.couchbase.client.scala.api._
import com.couchbase.client.core.retry.reactor.{Jitter, Retry, RetryContext}

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext

class Scenarios {
  private implicit val ec = ExecutionContext.Implicits.global

//  private val cluster = Cluster.connect("localhost", "Administrator", "password")
//  private val bucket = cluster.bucket("default")
//  private val scope = bucket.scope("scope")
//  private val coll = scope.collection("people")

  // Removing during implementation

//  def scenarioA(): Unit = {
//    val doc: GetResult = coll.get("id", timeout = 10.seconds).get
//
//    val content: JsonObject = doc.contentAsObject.put("field", "value")
//
//    val result: MutationResult = coll.replace(doc.id, content, doc.cas, timeout = 10.seconds)
//
//
//    // I include type annotations and readOrError above to make things clearer, but it'd be more idiomatic to write this:
//    // (note default params are supported for all methods along with the *Options builders)
//    coll.get("id", timeout = 10.seconds) match {
//      case Some(doc3) =>
//        coll.replace(doc3.id,
//          doc3.contentAsObject
//            .put("field", "value")
//            .put("foo", "bar"),
//          doc3.cas,
//          timeout = 10.seconds)
//
//      case _ => println("could not get doc")
//    }
//  }
//
//
//  def scenarioB(): Unit = {
//    val subdocOpt: Option[GetResult] = coll.get("id", GetSpec().get("someArray"), timeout = 10.seconds)
//
//    subdocOpt.map(subdoc => {
//      val arr: JsonArray = subdoc.contentAsArray
//      arr.add("foo")
//
//      val result: MutationResult = coll.mutateIn("id", MutateInSpec().upsert("someArray", arr), MutateInOptions().timeout(10.seconds))
//    })
//  }
//
//
//
//  def scenarioC_clientSideDurability(): Unit = {
//    // Use a helper wrapper to retry our operation in the face of durability failures
//    // remove is idempotent iff the app guarantees that the doc's id won't be reused (e.g. if it's a UUID).  This seems
//    // a reasonable restriction.
//    retryIdempotentRemoveClientSide((replicateTo: ObserveReplicateTo.Value) => {
//      val result: MutationResult = coll.remove("id", cas = 0, RemoveOptions().durabilityClient(replicateTo, ObservePersistTo.None))
//    }, ObserveReplicateTo.Two, ObserveReplicateTo.Two, System.nanoTime().nanos.plus(30.seconds))
//  }
//
//  /**
//    * Automatically retries an idempotent operation in the face of durability failures
//    * @param callback an idempotent remove operation to perform
//    * @param replicateTo the current ObserveReplicateTo setting being tried
//    * @param originalReplicateTo the originally requested ObserveReplicateTo setting
//    * @param until prevent the operation looping indefinitely
//    */
//  private def retryIdempotentRemoveClientSide(callback: (ObserveReplicateTo.Value) => Unit, replicateTo: ObserveReplicateTo.Value, originalReplicateTo: ObserveReplicateTo.Value, until: FiniteDuration): Unit = {
//    if (System.nanoTime().nanos >= until) {
//      // Depending on the durability requirements, may want to also log this to an external system for human review
//      // and reconciliation
//      throw new RuntimeException("Failed to durably write operation")
//    }
//
//    try {
//      callback(replicateTo)
//    }
//    catch {
//      case err: DocumentDoesNotExistException =>
//        println("Our work here is done")
//
//      case err: ReplicaNotConfiguredException =>
//        println("Not enough replicas configured, aborting")
//
//      case err: DocumentConcurrentlyModifiedException =>
//        // Just retry
//        retryIdempotentRemoveClientSide(callback, replicateTo, originalReplicateTo, until)
//
//      case err: DocumentMutationLostException =>
//        // Mutation lost during a hard failover.  I *think* we just retry with the original replicateTo.  If enough replicas
//        // still aren't available, it will presumably raise ReplicaNotAvailableException and retry with lower.
//        retryIdempotentRemoveClientSide(callback, originalReplicateTo, originalReplicateTo, until)
//
//      case err: ReplicaNotAvailableException =>
//        // Note this isn't necessary.  If replica is not available, the write will still go to as many as possible.
//        val newReplicateTo = replicateTo match {
//          case ObserveReplicateTo.One => ObserveReplicateTo.None
//          case ObserveReplicateTo.Two => ObserveReplicateTo.One
//          case ObserveReplicateTo.Three => ObserveReplicateTo.Two
//          case _ => ObserveReplicateTo.None
//        }
//        println("Temporary replica failure, retrying with lower durability " + newReplicateTo)
//        retryIdempotentRemoveClientSide(callback, newReplicateTo, originalReplicateTo, until)
//    }
//  }
//
//
//  def scenarioC_serverSideDurability(): Unit = {
//    // Use a helper wrapper to retry our operation in the face of durability failures
//    // remove is idempotent iff the app guarantees that the doc's id won't be reused (e.g. if it's a UUID).  This seems
//    // a reasonable restriction.
//    retryIdempotentRemoveServerSide(() => {
//      coll.remove("id", cas = 0, RemoveOptions().durabilityServer(Durability.MajorityAndPersistActive))
//    }, System.nanoTime().nanos.plus(30.seconds))
//  }
//
//  /**
//    * Automatically retries an idempotent operation in the face of durability failures
//    * @param callback an idempotent remove operation to perform
//    * @param until prevent the operation looping indefinitely
//    */
//  private def retryIdempotentRemoveServerSide(callback: () => Unit, until: FiniteDuration): Unit = {
//    if (System.nanoTime().nanos >= until) {
//      // Depending on the durability requirements, may want to also log this to an external system for human review
//      // and reconciliation
//      throw new RuntimeException("Failed to durably write operation")
//    }
//
//    try {
//      callback()
//    }
//    catch {
//      // Not entirely clear what failures need to be handled yet, but will be considerably easier than observe() based
//      // logic.  I think the only case to handle is:
//
//      case err: DurabilityAmbiguous =>
//        // A guarantee is that the mutation is either written to a majority of nodes, or none.  But we don't know which.
//        coll.get("id") match {
//          case Some(doc) => retryIdempotentRemoveServerSide(callback, until)
//          case _ => println("Our work here is done")
//        }
//
//      case err: DocumentDoesNotExistException =>
//        println("Our work here is done")
//    }
//  }
//
//
//  def scenarioD(): Unit = {
//    retryOperationOnCASMismatch(() => {
//      coll.get("id", timeout = 10.seconds) match {
//        case Some(doc) =>
//          coll.replace(doc.id,
//            doc.contentAsObject
//              .put("field", "value")
//              .put("foo", "bar"),
//            doc.cas,
//            timeout = 10.seconds)
//
//        case _ => println("could not get doc")
//      }
//    }, System.nanoTime().nanos.plus(30.seconds))
//  }
//
//  private def retryOperationOnCASMismatch(callback: () => Unit, until: FiniteDuration): Unit = {
//    if (System.nanoTime().nanos >= until) {
//        // What to do here is too app dependent to make a call on: return an error to the user, log for future human reconcilation, etc.
//      throw new RuntimeException("Failed to perform operation")
//    }
//
//    try {
//      callback()
//    }
//    catch {
//      case err: CASMismatchException => retryOperationOnCASMismatch(callback, until)
//    }
//  }
//
//
//  def scenarioE(): Unit = {
//    case class User(name: String, age: Int, address: String, phoneNumber: String)
//
//    coll.get("id", timeout = 10.seconds) match {
//      case Some(doc) =>
//        val user: User = doc.contentAs[User]
//        val changed = user.copy(age = 25)
//        coll.replace(doc.id, changed, doc.cas, timeout = 10.seconds)
//
//      case _ => println("could not get doc")
//    }
//  }
//
//
//  // {
//  //   user: {
//  //     name = "bob",
//  //     age = 23,
//  //     address = "123 Fake Street"
//  //   }
//  // }
//
//  def scenarioF_fulldoc(): Unit = {
//    case class User(name: String, age: Int, address: String)
//
//    coll.get("id") match {
//      case Some(doc) =>
//        val user: User = doc.contentAs[User]
//        val changed = user.copy(age = 25)
//
//        // Is this too dangerous?  What if a newer client has added 'phoneNumber' to user that this older client doesn't know about?
//        coll.replace(doc.id, changed, doc.cas)
//
//        coll.replace("id", changed, 0)
//
//      case _ => println("could not find doc")
//    }
//  }
//
//
//  def scenarioF_subdoc(): Unit = {
//    case class UserPartial(name: String, age: Int)
//
//    val subdoc: GetResult = coll.get("id", GetSpec().getMany("user.name", "user.age")).get
//
//    val user: UserPartial = subdoc.contentAs[UserPartial]
//    val changed = user.copy(age = 25)
//
//    // Note: I have misgivings over whether this update-with-a-projection should be allowed
//    // mergeUpsert will upsert fields user.name & user.age, leaving user.address alone
//    coll.mutateIn(subdoc.id, MutateInSpec().mergeUpsert("user", changed))
//  }
}
