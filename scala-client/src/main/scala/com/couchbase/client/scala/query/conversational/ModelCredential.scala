package com.couchbase.client.scala.query.conversational

/** The credential the query service uses to authenticate with the model provider.
  *
  * This is separate from Couchbase authentication.
  */
sealed trait ModelCredential

object ModelCredential {

  /** Authenticates via Capella.
    *
    * @param email          a Capella account email
    * @param password       a Capella account password
    * @param organizationId a Capella organization id
    */
  case class Capella(email: String, password: String, organizationId: String)
      extends ModelCredential {
    override def toString: String = s"Capella(<redacted>)"
  }

  /** Uses a credential previously stored in the query service, directly with the model provider.
    *
    * @param credentialName the name of the stored credential
    */
  case class CredentialStore(credentialName: String) extends ModelCredential

  /** Uses an API key, directly with the model provider.
    *
    * @param key the API key
    */
  case class ApiKey(key: String) extends ModelCredential {
    override def toString: String = "ApiKey(<redacted>)"
  }
}
