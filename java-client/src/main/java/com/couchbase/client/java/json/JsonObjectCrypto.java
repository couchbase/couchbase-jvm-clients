/*
 * Copyright 2020 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.java.json;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.JsonProcessingException;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.encryption.CryptoManager;
import com.couchbase.client.core.json.Mapper;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Set;
import java.util.stream.Collectors;

import static com.couchbase.client.core.util.CbObjects.defaultIfNull;
import static java.util.Objects.requireNonNull;

/**
 * A view of a Couchbase {@link JsonObject} for reading and writing encrypted fields.
 * <p>
 * The methods of this class mirror the methods of {@code JsonObject},
 * and behave the same way except they operate on encrypted values.
 * Values read via the crypto object are decrypted, and values written
 * via the view are encrypted.
 * <p>
 * The JsonObjectCrypto view can only see encrypted fields. Attempting to
 * read an unencrypted field via the view has the same result as if the field
 * does not exist.
 * <p>
 * New instances are created by calling {@link JsonObject#crypto}.
 * <p>
 * Example usage:
 * <pre>
 * Collection collection = cluster.bucket("myBucket").defaultCollection();
 *
 * JsonObject document = JsonObject.create();
 * JsonObjectCrypto crypto = document.crypto(collection);
 * crypto.put("locationOfBuriedTreasure", "Between palm trees");
 *
 * // This displays the encrypted form of the field
 * System.out.println(document);
 *
 * collection.upsert("treasureMap", document);
 *
 * JsonObject readItBack = collection.get("treasureMap").contentAsObject();
 * JsonObjectCrypto readItBackCrypto = readItBack.crypto(collection);
 * System.out.println(readItBackCrypto.getString("locationOfBuriedTreasure"));
 * </pre>
 */
@Stability.Volatile
public class JsonObjectCrypto {

  private final CryptoManager cryptoManager;
  private final String encrypterAlias;
  private final JsonObject wrapped;

  /**
   * @param cryptoManager handles the actual encryption and decryption
   * @param encrypterAlias (nullable) alias of the encrypter to use for writing fields,
   * or null for default encrypter.
   */
  protected JsonObjectCrypto(JsonObject jsonObject, CryptoManager cryptoManager, String encrypterAlias) {
    this.wrapped = requireNonNull(jsonObject);
    this.cryptoManager = requireNonNull(cryptoManager);
    this.encrypterAlias = defaultIfNull(encrypterAlias, CryptoManager.DEFAULT_ENCRYPTER_ALIAS);
  }

  /**
   * Returns a new {@code JsonObjectCrypto} instance that uses the decrypter identified by the given alias.
   */
  public JsonObjectCrypto withEncrypter(String encrypterAlias) {
    return new JsonObjectCrypto(wrapped, cryptoManager, encrypterAlias);
  }

  /**
   * Returns a new {@code JsonObjectCrypto} instance that uses the default encrypter.
   */
  public JsonObjectCrypto withDefaultEncrypter() {
    return new JsonObjectCrypto(wrapped, cryptoManager, null);
  }

  /**
   * Returns a new instance that is a view of the given JsonObject.
   * <p>
   * The returned instance uses the same {@link CryptoManager} and encrypter alias as this JsonObjectCrypto instance.
   */
  public JsonObjectCrypto withObject(JsonObject object) {
    return new JsonObjectCrypto(object, this.cryptoManager, this.encrypterAlias);
  }

  /**
   * Returns the JsonObject bound to this crypto view.
   */
  public JsonObject object() {
    return wrapped;
  }

  public boolean hasEncryptedField(String fieldName) {
    return wrapped.getNames().contains(cryptoManager.mangle(fieldName));
  }

  /**
   * Returns the demangled names of all encrypted fields.
   */
  public Set<String> getEncryptedFieldNames() {
    return wrapped.getNames().stream()
        .filter(cryptoManager::isMangled)
        .map(cryptoManager::demangle)
        .collect(Collectors.toSet());
  }

  /**
   * Returns the names of all unencrypted fields.
   */
  public Set<String> getUnencryptedFieldNames() {
    return wrapped.getNames().stream()
        .filter(name -> !cryptoManager.isMangled(name))
        .collect(Collectors.toSet());
  }

  public JsonObjectCrypto put(String fieldName, Object fieldValue) {
    if (wrapped == fieldValue) {
      throw new IllegalArgumentException("Cannot put self");
    }

    fieldValue = JsonValue.coerce(fieldValue);

    try {
      byte[] plaintext = mapper().writeValueAsBytes(fieldValue);
      wrapped.put(cryptoManager.mangle(fieldName), cryptoManager.encrypt(plaintext, encrypterAlias));
      return this;

    } catch (JsonProcessingException e) {
      throw new RuntimeException("JSON serialization failed", e);
    }
  }

  /**
   * Returns a new JsonObject containing only the decrypted version of the requested field,
   * or an empty object if the requested field is absent.
   *
   * @param fieldName name of the field to decrypt. The name must not be mangled (it will be mangled internal to this method).
   * @implNote The new JsonObject is created so the caller can get the value using
   * the normal JsonObject accessors. As a result, this crypto accessors exactly mirror
   * the behavior of the normal accessors.
   */
  private JsonObject decrypt(String fieldName) {
    JsonObject encryptedValue = wrapped.getObject(cryptoManager.mangle(fieldName));
    if (encryptedValue == null) {
      return JsonObject.create();
    }

    byte[] plaintext = cryptoManager.decrypt(encryptedValue.toMap());

    ObjectNode decrypted = mapper().createObjectNode()
        .set(fieldName, Mapper.decodeIntoTree(plaintext));

    return mapper().convertValue(decrypted, JsonObject.class);
  }

  private static ObjectMapper mapper() {
    return JacksonTransformers.MAPPER;
  }

  public Object get(String fieldName) {
    return decrypt(fieldName).get(fieldName);
  }

  public JsonArray getArray(String fieldName) {
    return decrypt(fieldName).getArray(fieldName);
  }

  public JsonObject getObject(String fieldName) {
    return decrypt(fieldName).getObject(fieldName);
  }

  public String getString(String fieldName) {
    return decrypt(fieldName).getString(fieldName);
  }

  public Boolean getBoolean(String fieldName) {
    return decrypt(fieldName).getBoolean(fieldName);
  }

  public Integer getInt(String fieldName) {
    return decrypt(fieldName).getInt(fieldName);
  }

  public Long getLong(String fieldName) {
    return decrypt(fieldName).getLong(fieldName);
  }

  public Double getDouble(String fieldName) {
    return decrypt(fieldName).getDouble(fieldName);
  }

  public Number getNumber(String fieldName) {
    return decrypt(fieldName).getNumber(fieldName);
  }

  public BigDecimal getBigDecimal(String fieldName) {
    return decrypt(fieldName).getBigDecimal(fieldName);
  }

  public BigInteger getBigInteger(String fieldName) {
    return decrypt(fieldName).getBigInteger(fieldName);
  }

  public JsonObjectCrypto remove(String fieldName) {
    wrapped.removeKey(cryptoManager.mangle(fieldName));
    return this;
  }

  /**
   * Returns the String representation of the bound JsonObject.
   */
  @Override
  public String toString() {
    return wrapped.toString();
  }
}
