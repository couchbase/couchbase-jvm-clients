package com.couchbase.client.scala.query.conversational

import com.couchbase.client.core.CoreKeyspace
import com.couchbase.client.core.api.query.{
  CoreAskOptions,
  CoreBeginChatOptions,
  CoreChatState,
  CoreConversationalQueryOps,
  CoreModelCredential,
  CoreModelOptions,
  CorePauseOptions
}
import com.couchbase.client.core.endpoint.http.CoreCommonOptions
import com.couchbase.client.core.error.InvalidArgumentException
import com.couchbase.client.scala.Collection
import com.couchbase.client.scala.json.JsonObject
import com.couchbase.client.scala.query.{QueryProfile, QueryScanConsistency}
import com.couchbase.client.scala.util.CoreCommonConverters.convert
import com.couchbase.client.scala.util.DurationConversions

import java.time
import java.util.Optional
import scala.compat.java8.OptionConverters._
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

/** Provides conversational query: submitting natural-language prompts to the query service, which generates statements
  * and can execute them.
  *
  * Supports both one-shot queries, with [[query]], and multi-turn chats, with [[beginChat]] and [[attachChat]].
  *
  * Conversational query is not supported with `couchbase2://` connections.  Chats are additionally not supported on
  * Capella Private Link.
  *
  * Obtained with `Cluster.conversationalQuery()`.
  */
class ConversationalQuery private[scala] (private val ops: CoreConversationalQueryOps) {
  import ConversationalQuery._

  type Keyspaces = Seq[Collection]

  /** Performs a one-shot conversational query: the query service generates a statement from the prompt and, unless
    * disabled with [[ConversationalQueryOptions.execute]], may execute it.
    *
    * @param prompt    the natural-language prompt
    * @param keyspaces the collections the query service may use.  Must not be empty
    * @param options   any options - see [[ConversationalQueryOptions]] for documentation
    *
    * @return a `Try` containing a `Success(ConversationalQueryResult)` (which includes any generated statement and
    *         returned rows) if successful, else a `Failure`
    */
  def query(
      prompt: String,
      keyspaces: Keyspaces,
      options: ConversationalQueryOptions = ConversationalQueryOptions.Default
  ): Try[ConversationalQueryResult] = {
    Try(ops.queryBlocking(prompt, toCore(keyspaces), options.toCore))
      .map(result => convert(result))
  }

  /** Begins a new multi-turn chat, on a query node selected by the SDK.
    *
    * Any model and credential in the options are used for all operations on the returned `Chat`.
    *
    * @param keyspaces the collections the query service may use for the chat.  Must not be empty
    * @param options   any options - see [[ConversationalQuery.BeginChatOptions]] for documentation
    *
    * @return a `Try` containing a `Success(Chat)` if successful, else a `Failure`
    */
  def beginChat(
      keyspaces: Keyspaces,
      options: BeginChatOptions = BeginChatOptions.Default
  ): Try[Chat] = {
    Try(ops.beginChatBlocking(toCore(keyspaces), options.toCore))
      .map(result =>
        new Chat(
          ops,
          new CoreChatState(result.chatId, result.ownerNodeUuid),
          options.model,
          options.credential
        )
      )
  }

  /** Begins a new multi-turn chat, on a query node selected by the SDK.
    *
    * A convenience overload, equivalent to providing the model and credential with
    * [[ConversationalQuery.BeginChatOptions]].
    *
    * @param keyspaces  the collections the query service may use for the chat.  Must not be empty
    * @param model      the model configuration for the returned `Chat`.
    * @param credential the model credential for the returned `Chat`.
    *
    * @return a `Try` containing a `Success(Chat)` if successful, else a `Failure`
    */
  def beginChat(keyspaces: Keyspaces, model: ModelOptions, credential: ModelCredential): Try[Chat] =
    beginChat(keyspaces, BeginChatOptions().model(model).credential(credential))

  /** Creates a `Chat` from a previously saved [[ConversationalQuery.ChatHandle]], to continue that chat.
    *
    * This is a local operation and performs no server I/O.  In particular, it does not resume a paused chat: use
    * [[ConversationalQuery.Chat.resume]] for that.
    *
    * Crucial: The returned `Chat` takes its model and credential only from the options.  It does not use
    * the model and credentials that were previously provided.
    *
    * @param handle  the handle of the chat to continue
    * @param options any options - see [[ConversationalQuery.AttachChatOptions]] for documentation
    *
    * @return the `Chat`
    */
  def attachChat(
      handle: ChatHandle,
      options: AttachChatOptions = AttachChatOptions.Default
  ): Chat =
    new Chat(
      ops,
      new CoreChatState(handle.chatId, handle.nodeUuid.orNull),
      options.model,
      options.credential
    )

  /** Creates a `Chat` from a previously saved [[ConversationalQuery.ChatHandle]], to continue that chat.
    *
    * A convenience overload, equivalent to providing the model and credential with
    * [[ConversationalQuery.AttachChatOptions]].
    *
    * This is a local operation and performs no server I/O.  In particular, it does not resume a paused chat: use
    * [[ConversationalQuery.Chat.resume]] for that.
    *
    * @param handle     the handle of the chat to continue
    * @param model      the model configuration for the returned `Chat`
    * @param credential the model credential for the returned `Chat`
    *
    * @return the `Chat`
    */
  def attachChat(handle: ChatHandle, model: ModelOptions, credential: ModelCredential): Chat =
    attachChat(handle, AttachChatOptions().model(model).credential(credential))
}

object ConversationalQuery {

  private[scala] def toCore(keyspaces: Seq[Collection]): java.util.List[CoreKeyspace] =
    keyspaces.map(_.async.keyspace).asJava

  /** Customize a [[ConversationalQuery.beginChat]] operation. */
  case class BeginChatOptions(
      private[scala] val model: Option[ModelOptions] = None,
      private[scala] val credential: Option[ModelCredential] = None,
      private[scala] val inactivityTimeout: Option[Duration] = None,
      private[scala] val useKnowledge: Option[Boolean] = None,
      private[scala] val timeout: Option[Duration] = None
  ) {

    /** Sets the model configuration used for all operations on the returned `Chat`.
      *
      * It is not sent with the begin request itself.
      *
      * @return a copy of this with the change applied, for chaining.
      */
    def model(value: ModelOptions): BeginChatOptions = copy(model = Some(value))

    /** Sets the model credential used for all operations on the returned `Chat`.
      *
      * It is not sent with the begin request itself.
      *
      * @return a copy of this with the change applied, for chaining.
      */
    def credential(value: ModelCredential): BeginChatOptions = copy(credential = Some(value))

    /** Sets how long the chat may be inactive before the query service expires it.
      *
      * @return a copy of this with the change applied, for chaining.
      */
    def inactivityTimeout(value: Duration): BeginChatOptions = copy(inactivityTimeout = Some(value))

    /** Controls whether the query service uses its configured Knowledge entries for this chat.
      *
      * @return a copy of this with the change applied, for chaining.
      */
    def useKnowledge(value: Boolean): BeginChatOptions = copy(useKnowledge = Some(value))

    /** Sets a maximum timeout for processing.
      *
      * Note that a timeout reported by the SDK does not guarantee that the query service has stopped processing the
      * request.
      *
      * @return a copy of this with the change applied, for chaining.
      */
    def timeout(value: Duration): BeginChatOptions = copy(timeout = Some(value))

    private[scala] def toCore: CoreBeginChatOptions = {
      val x = this

      val common = CoreCommonOptions.ofOptional(
        timeout.map(v => DurationConversions.scalaDurationToJava(v)).asJava,
        Optional.empty(),
        Optional.empty()
      )

      new CoreBeginChatOptions {
        override def model(): CoreModelOptions = x.model.map(_.toCore).orNull

        override def credential(): CoreModelCredential = x.credential.map(convert).orNull

        override def inactivityTimeout(): time.Duration =
          x.inactivityTimeout.map(v => DurationConversions.scalaDurationToJava(v)).orNull

        override def useKnowledge(): java.lang.Boolean = x.useKnowledge.map(Boolean.box).orNull

        override def commonOptions(): CoreCommonOptions = common
      }
    }
  }

  object BeginChatOptions {
    val Default = BeginChatOptions()
  }

  /** Customize a [[ConversationalQuery.attachChat]] operation. */
  case class AttachChatOptions(
      private[scala] val model: Option[ModelOptions] = None,
      private[scala] val credential: Option[ModelCredential] = None
  ) {

    /** Sets the model configuration used for all operations on the returned `Chat`.
      *
      * @return a copy of this with the change applied, for chaining.
      */
    def model(value: ModelOptions): AttachChatOptions = copy(model = Some(value))

    /** Sets the model credential used for all operations on the returned `Chat`.
      *
      * @return a copy of this with the change applied, for chaining.
      */
    def credential(value: ModelCredential): AttachChatOptions = copy(credential = Some(value))
  }

  object AttachChatOptions {
    val Default = AttachChatOptions()
  }

  /** An opaque, serializable snapshot of the client state needed to continue and route a chat.
    */
  case class ChatHandle(
      private[scala] val nodeUuid: Option[String],
      private[scala] val chatId: String
  ) {

    /** Serializes this handle to JSON, so it can be later restored with [[ChatHandle.fromJson]]. */
    def toJson(): String = {
      JsonObject.create.put("chatId", chatId).put("owner", nodeUuid.orNull).toString
    }
  }

  object ChatHandle {

    private[scala] def snapshot(state: CoreChatState): Try[ChatHandle] = {
      if (state.closed) {
        Failure(
          new InvalidArgumentException(
            "This chat has been ended, so cannot be used further",
            null,
            null
          )
        )
      } else {
        Success(ChatHandle(Option(state.owner), state.chatId))
      }
    }

    /** Restores a handle previously serialized with [[ChatHandle.toJson]].
      *
      * Fails with `InvalidArgumentException` if the handle is malformed.
      */
    def fromJson(json: String): Try[ChatHandle] = {
      Try(JsonObject.fromJson(json)).flatMap(converted => {
        // Raw values, so fields of the wrong type are rejected rather than converted.
        def field(name: String): Option[Any] =
          if (converted != null && converted.containsKey(name)) Some(converted.get(name)) else None

        // Both fields are required, but owner may be null (on a PAUSE-d chat, for instance).  Unrecognized fields are ignored.
        (field("chatId"), field("owner")) match {
          case (Some(chatId: String), Some(owner: String)) =>
            Success(ChatHandle(Some(owner), chatId))
          case (Some(chatId: String), Some(null)) => Success(ChatHandle(None, chatId))
          case _                                  =>
            Failure(new InvalidArgumentException(s"Malformed ChatHandle: $json", null, null))
        }
      })
    }
  }

  /** A multi-turn conversational query chat.
    *
    * Created with [[ConversationalQuery.beginChat]] or [[ConversationalQuery.attachChat]].
    *
    * Applications must serialize `pause`, `resume` and `end` operations on the same chat, including across `Chat`
    * instances.
    *
    * The model and credential are fixed when the `Chat` is created.  To use different ones with the same chat, create
    * a new `Chat` with `attachChat(chat.handle().get, options)`.
    */
  class Chat private[scala] (
      private val ops: CoreConversationalQueryOps,
      private val state: CoreChatState,
      private val model: Option[ModelOptions],
      private val credential: Option[ModelCredential]
  ) {

    /** Sends a natural-language prompt as the next turn of this chat.  The query service generates a statement and,
      * unless disabled with [[Chat.AskOptions.execute]], may execute it.
      *
      * @param prompt  the natural-language prompt
      * @param options any options - see [[Chat.AskOptions]] for documentation
      *
      * @return a `Try` containing a `Success(ConversationalQueryResult)` (which includes any generated statement and
      *         returned rows) if successful, else a `Failure`
      */
    def ask(
        prompt: String,
        options: Chat.AskOptions = Chat.AskOptions.Default
    ): Try[ConversationalQueryResult] = {
      Try(ops.askBlocking(state, prompt, options.toCore(model, credential)))
        .map(result => convert(result))
    }

    /** Pauses this chat, so the query service persists it until it is resumed or expires.
      *
      * The query service may use the model to summarize the conversation first; see [[Chat.PauseOptions.summarize]].
      *
      * @param options any options - see [[Chat.PauseOptions]] for documentation
      *
      * @return a `Try` containing a `Success(ConversationalQueryMetaData)` if successful, else a `Failure`
      */
    def pause(
        options: Chat.PauseOptions = Chat.PauseOptions.Default
    ): Try[ConversationalQueryMetaData] = {
      Try(ops.pauseBlocking(state, options.toCore(model, credential)))
        .map(result => convert(result))
    }

    /** Resumes this chat, restoring it to a live state on a query node.
      *
      * This can be called even if the chat is believed to be live, since the SDK's routing information may be stale.
      *
      * @param options any options - see [[Chat.ResumeOptions]] for documentation
      *
      * @return a `Try` containing a `Success(ConversationalQueryMetaData)` if successful, else a `Failure`
      */
    def resume(
        options: Chat.ResumeOptions = Chat.ResumeOptions.Default
    ): Try[ConversationalQueryMetaData] = {
      Try(ops.resumeBlocking(state, options.toCore))
        .map(result => convert(result))
    }

    /** Ends this chat.
      *
      * This does not delete a paused chat, which must be resumed first.
      *
      * After this succeeds, this `Chat` is closed, and further operations on it fail with `InvalidArgumentException`.
      *
      * @param options any options - see [[Chat.EndOptions]] for documentation
      *
      * @return a `Try` containing a `Success(ConversationalQueryMetaData)` if successful, else a `Failure`
      */
    def end(
        options: Chat.EndOptions = Chat.EndOptions.Default
    ): Try[ConversationalQueryMetaData] = {
      Try(ops.endBlocking(state, options.toCore))
        .map(result => convert(result))
    }

    /** Returns a snapshot of this chat's current handle, which can be saved and later passed to
      * [[ConversationalQuery.attachChat]] to continue the chat.
      *
      * Note that it is a snapshot in time.
      *
      * @return a `Try` containing a `Success(ChatHandle)`, else a `Failure(InvalidArgumentException)` if this `Chat`
      *         has been ended
      */
    def handle(): Try[ChatHandle] = ChatHandle.snapshot(state)
  }

  object Chat {

    private def commonOptions(timeout: Option[Duration]): CoreCommonOptions =
      CoreCommonOptions.ofOptional(
        timeout.map(v => DurationConversions.scalaDurationToJava(v)).asJava,
        Optional.empty(),
        Optional.empty()
      )

    /** Customize a [[Chat.ask]] operation. */
    case class AskOptions(
        private[scala] val hint: Option[String] = None,
        private[scala] val execute: Option[Boolean] = None,
        private[scala] val output: Option[ConversationalQueryOutput] = None,
        private[scala] val scanConsistency: Option[QueryScanConsistency] = None,
        private[scala] val useReplica: Option[Boolean] = None,
        private[scala] val clientContextId: Option[String] = None,
        private[scala] val metrics: Option[Boolean] = None,
        private[scala] val profile: Option[QueryProfile] = None,
        private[scala] val timeout: Option[Duration] = None,
        private[scala] val rawParameters: Option[Map[String, Any]] = None
    ) {

      /** Provides an additional hint to the model.
        *
        * @return a copy of this with the change applied, for chaining.
        */
      def hint(value: String): AskOptions = copy(hint = Some(value))

      /** Controls whether the query service executes the generated statement.
        *
        * Note that `execute(true)` does not guarantee execution.  To determine whether the statement was executed,
        * enable [[metrics]] and inspect [[ConversationalQueryMetaData.generatedStatementExecuted]].
        *
        * @return a copy of this with the change applied, for chaining.
        */
      def execute(value: Boolean): AskOptions = copy(execute = Some(value))

      /** Sets the type of statement to generate.
        *
        * @return a copy of this with the change applied, for chaining.
        */
      def output(value: ConversationalQueryOutput): AskOptions = copy(output = Some(value))

      /** Scan consistency for any executed statement.
        *
        * A scan wait can be provided with `QueryScanConsistency.RequestPlus(Some(scanWait))`.
        *
        * @return a copy of this with the change applied, for chaining.
        */
      def scanConsistency(value: QueryScanConsistency): AskOptions =
        copy(scanConsistency = Some(value))

      /** Tells the query engine that replicas can be used.
        *
        * @return a copy of this with the change applied, for chaining.
        */
      def useReplica(value: Boolean): AskOptions = copy(useReplica = Some(value))

      /** Adds a client context ID to the request, that will be sent back in the response, allowing clients
        * to meaningfully trace requests/responses when many are exchanged.
        *
        * @return a copy of this with the change applied, for chaining.
        */
      def clientContextId(value: String): AskOptions = copy(clientContextId = Some(value))

      /** Controls whether metrics are returned by the server.
        *
        * @return a copy of this with the change applied, for chaining.
        */
      def metrics(value: Boolean): AskOptions = copy(metrics = Some(value))

      /** Set the profiling information level for query execution.  It can be retrieved with
        * [[ConversationalQueryMetaData.profileAs]].
        *
        * This is an Enterprise Edition feature.  On Community Edition the parameter will be accepted, but no profiling
        * information returned.
        *
        * @return a copy of this with the change applied, for chaining.
        */
      def profile(value: QueryProfile): AskOptions = copy(profile = Some(value))

      /** Sets a maximum timeout for processing.
        *
        * Note that a timeout reported by the SDK does not guarantee that the query service has stopped processing the
        * request.
        *
        * @return a copy of this with the change applied, for chaining.
        */
      def timeout(value: Duration): AskOptions = copy(timeout = Some(value))

      /** Allows providing custom query request parameters, for advanced usage.
        *
        * Parameter names beginning with `natural` (case-insensitively) are reserved and will fail the operation.
        *
        * Note that the values will be passed through a JSON encoder, so do not provide already encoded JSON as the
        * value. If you want to pass objects or arrays, you can use `JsonObject` and `JsonArray` respectively.
        *
        * @return a copy of this with the change applied, for chaining.
        */
      def rawParameters(value: Map[String, Any]): AskOptions = copy(rawParameters = Some(value))

      private[scala] def toCore(
          chatModel: Option[ModelOptions],
          chatCredential: Option[ModelCredential]
      ): CoreAskOptions = {
        ConversationalQueryOptions(
          model = chatModel,
          credential = chatCredential,
          hint = hint,
          execute = execute,
          output = output,
          scanConsistency = scanConsistency,
          useReplica = useReplica,
          clientContextId = clientContextId,
          metrics = metrics,
          profile = profile,
          timeout = timeout,
          rawParameters = rawParameters
        ).toCore
      }
    }

    object AskOptions {
      val Default = AskOptions()
    }

    /** Customize a [[Chat.pause]] operation. */
    case class PauseOptions(
        private[scala] val summarize: Option[Boolean] = None,
        private[scala] val timeout: Option[Duration] = None
    ) {

      /** Controls whether the query service may summarize the conversation, using the model, before saving it.
        *
        * If unset or true, the model may be invoked.  If false, no model inputs are sent.
        *
        * @return a copy of this with the change applied, for chaining.
        */
      def summarize(value: Boolean): PauseOptions = copy(summarize = Some(value))

      /** Sets a maximum timeout for processing.
        *
        * Note that a timeout reported by the SDK does not guarantee that the query service has stopped processing the
        * request.
        *
        * @return a copy of this with the change applied, for chaining.
        */
      def timeout(value: Duration): PauseOptions = copy(timeout = Some(value))

      private[scala] def toCore(
          chatModel: Option[ModelOptions],
          chatCredential: Option[ModelCredential]
      ): CorePauseOptions = {
        val x      = this
        val common = commonOptions(timeout)

        new CorePauseOptions {
          override def model(): CoreModelOptions = chatModel.map(_.toCore).orNull

          override def credential(): CoreModelCredential = chatCredential.map(convert).orNull

          override def summarize(): java.lang.Boolean = x.summarize.map(Boolean.box).orNull

          override def commonOptions(): CoreCommonOptions = common
        }
      }
    }

    object PauseOptions {
      val Default = PauseOptions()
    }

    /** Customize a [[Chat.resume]] operation. */
    case class ResumeOptions(private[scala] val timeout: Option[Duration] = None) {

      /** Sets a maximum timeout for processing.
        *
        * Note that a timeout reported by the SDK does not guarantee that the query service has stopped processing the
        * request.
        *
        * @return a copy of this with the change applied, for chaining.
        */
      def timeout(value: Duration): ResumeOptions = copy(timeout = Some(value))

      private[scala] def toCore: CoreCommonOptions = commonOptions(timeout)
    }

    object ResumeOptions {
      val Default = ResumeOptions()
    }

    /** Customize a [[Chat.end]] operation. */
    case class EndOptions(private[scala] val timeout: Option[Duration] = None) {

      /** Sets a maximum timeout for processing.
        *
        * Note that a timeout reported by the SDK does not guarantee that the query service has stopped processing the
        * request.
        *
        * @return a copy of this with the change applied, for chaining.
        */
      def timeout(value: Duration): EndOptions = copy(timeout = Some(value))

      private[scala] def toCore: CoreCommonOptions = commonOptions(timeout)
    }

    object EndOptions {
      val Default = EndOptions()
    }
  }
}
