package com.couchbase.client.scala.query.conversational

import com.couchbase.client.core.api.query.{CoreChatState, CoreConversationalQueryOps}
import com.couchbase.client.scala.Collection
import com.couchbase.client.scala.query.conversational.ConversationalQuery.{
  AttachChatOptions,
  BeginChatOptions,
  Chat,
  ChatHandle
}
import com.couchbase.client.scala.query.conversational.AsyncConversationalQuery.AsyncChat
import com.couchbase.client.scala.util.CoreCommonConverters.convert

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

/** The asynchronous version of [[ConversationalQuery]], returning `Future`s.
  *
  * Obtained with `AsyncCluster.conversationalQuery()`.
  */
class AsyncConversationalQuery private[scala] (private val ops: CoreConversationalQueryOps)(
    implicit ec: ExecutionContext
) {
  type Keyspaces = Seq[Collection]

  /** Performs a one-shot conversational query: the query service generates a statement from the prompt and, unless
    * disabled with [[ConversationalQueryOptions.execute]], may execute it.
    *
    * @param prompt    the natural-language prompt
    * @param keyspaces the collections the query service may use.  Must not be empty
    * @param options   any options - see [[ConversationalQueryOptions]] for documentation
    *
    * @return a `Future` containing a `Success(ConversationalQueryResult)` (which includes any generated statement and
    *         returned rows) if successful, else a `Failure`
    */
  def query(
      prompt: String,
      keyspaces: Keyspaces,
      options: ConversationalQueryOptions = ConversationalQueryOptions.Default
  ): Future[ConversationalQueryResult] = {
    convert(ops.queryAsync(prompt, ConversationalQuery.toCore(keyspaces), options.toCore))
      .map(result => convert(result))
  }

  /** Begins a new multi-turn chat, on a query node selected by the SDK.
    *
    * Any model and credential in the options are used for all operations on the returned `AsyncChat`.
    *
    * @param keyspaces the collections the query service may use for the chat.  Must not be empty
    * @param options   any options - see [[ConversationalQuery.BeginChatOptions]] for documentation
    *
    * @return a `Future` containing a `Success(AsyncChat)` if successful, else a `Failure`
    */
  def beginChat(
      keyspaces: Keyspaces,
      options: BeginChatOptions = BeginChatOptions.Default
  ): Future[AsyncChat] = {
    convert(ops.beginChatAsync(ConversationalQuery.toCore(keyspaces), options.toCore))
      .map(result =>
        new AsyncChat(
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
    * @param model      the model configuration for the returned `AsyncChat`.  It is not sent with this request
    * @param credential the model credential for the returned `AsyncChat`.  It is not sent with this request
    *
    * @return a `Future` containing a `Success(AsyncChat)` if successful, else a `Failure`
    */
  def beginChat(
      keyspaces: Keyspaces,
      model: ModelOptions,
      credential: ModelCredential
  ): Future[AsyncChat] =
    beginChat(keyspaces, BeginChatOptions().model(model).credential(credential))

  /** Creates an `AsyncChat` from a previously saved [[ConversationalQuery.ChatHandle]], to continue that chat.
    *
    * This is a local operation and performs no server I/O.  In particular, it does not resume a paused chat: use
    * [[AsyncConversationalQuery.AsyncChat.resume]] for that.
    *
    * Crucial: The returned `Chat` takes its model and credential only from the options.  It does not use
    * the model and credentials that were previously provided.
    *
    * @param handle  the handle of the chat to continue
    * @param options any options - see [[ConversationalQuery.AttachChatOptions]] for documentation
    *
    * @return the `AsyncChat`
    */
  def attachChat(
      handle: ChatHandle,
      options: AttachChatOptions = AttachChatOptions.Default
  ): AsyncChat =
    new AsyncChat(
      ops,
      new CoreChatState(handle.chatId, handle.nodeUuid.orNull),
      options.model,
      options.credential
    )

  /** Creates an `AsyncChat` from a previously saved [[ConversationalQuery.ChatHandle]], to continue that chat.
    *
    * A convenience overload, equivalent to providing the model and credential with
    * [[ConversationalQuery.AttachChatOptions]].
    *
    * This is a local operation and performs no server I/O.  In particular, it does not resume a paused chat: use
    * [[AsyncConversationalQuery.AsyncChat.resume]] for that.
    *
    * @param handle     the handle of the chat to continue
    * @param model      the model configuration for the returned `AsyncChat`
    * @param credential the model credential for the returned `AsyncChat`
    *
    * @return the `AsyncChat`
    */
  def attachChat(handle: ChatHandle, model: ModelOptions, credential: ModelCredential): AsyncChat =
    attachChat(handle, AttachChatOptions().model(model).credential(credential))
}

object AsyncConversationalQuery {

  /** The asynchronous version of [[ConversationalQuery.Chat]], returning `Future`s.
    *
    * Created with [[AsyncConversationalQuery.beginChat]] or [[AsyncConversationalQuery.attachChat]].
    *
    * Applications must serialize `pause`, `resume` and `end` operations on the same chat, including across chat
    * instances.
    */
  class AsyncChat private[scala] (
      private val ops: CoreConversationalQueryOps,
      private val state: CoreChatState,
      private val model: Option[ModelOptions],
      private val credential: Option[ModelCredential]
  )(implicit ec: ExecutionContext) {

    /** Sends a natural-language prompt as the next turn of this chat.  The query service generates a statement and,
      * unless disabled with [[ConversationalQuery.Chat.AskOptions.execute]], may execute it.
      *
      * @param prompt  the natural-language prompt
      * @param options any options - see [[ConversationalQuery.Chat.AskOptions]] for documentation
      *
      * @return a `Future` containing a `Success(ConversationalQueryResult)` (which includes any generated statement and
      *         returned rows) if successful, else a `Failure`
      */
    def ask(
        prompt: String,
        options: Chat.AskOptions = Chat.AskOptions.Default
    ): Future[ConversationalQueryResult] = {
      convert(ops.askAsync(state, prompt, options.toCore(model, credential)))
        .map(result => convert(result))
    }

    /** Pauses this chat, so the query service persists it until it is resumed or expires.
      *
      * The query service may use the model to summarize the conversation first; see
      * [[ConversationalQuery.Chat.PauseOptions.summarize]].
      *
      * @param options any options - see [[ConversationalQuery.Chat.PauseOptions]] for documentation
      *
      * @return a `Future` containing a `Success(ConversationalQueryMetaData)` if successful, else a `Failure`
      */
    def pause(
        options: Chat.PauseOptions = Chat.PauseOptions.Default
    ): Future[ConversationalQueryMetaData] = {
      convert(ops.pauseAsync(state, options.toCore(model, credential)))
        .map(result => convert(result))
    }

    /** Resumes this chat, restoring it to a live state on a query node.
      *
      * @param options any options - see [[ConversationalQuery.Chat.ResumeOptions]] for documentation
      *
      * @return a `Future` containing a `Success(ConversationalQueryMetaData)` if successful, else a `Failure`
      */
    def resume(
        options: Chat.ResumeOptions = Chat.ResumeOptions.Default
    ): Future[ConversationalQueryMetaData] = {
      convert(ops.resumeAsync(state, options.toCore))
        .map(result => convert(result))
    }

    /** Ends this chat.
      *
      * This does not delete a paused chat - that must be resumed first.
      *
      * After this succeeds, this `AsyncChat` is closed, and further operations on it fail with
      * `InvalidArgumentException`.
      *
      * @param options any options - see [[ConversationalQuery.Chat.EndOptions]] for documentation
      *
      * @return a `Future` containing a `Success(ConversationalQueryMetaData)` if successful, else a `Failure`
      */
    def end(
        options: Chat.EndOptions = Chat.EndOptions.Default
    ): Future[ConversationalQueryMetaData] = {
      convert(ops.endAsync(state, options.toCore))
        .map(result => convert(result))
    }

    /** Returns a snapshot of this chat's current handle, which can be saved and later passed to
      * [[AsyncConversationalQuery.attachChat]] or [[ConversationalQuery.attachChat]] to continue the chat.
      *
      * Note that it is a snapshot in time.
      *
      * This is a local operation.
      *
      * @return a `Try` containing a `Success(ChatHandle)`, else a `Failure(InvalidArgumentException)` if this chat
      *         has been ended
      */
    def handle(): Try[ChatHandle] = ChatHandle.snapshot(state)
  }
}
