package com.couchbase.client.java.json;

import com.couchbase.client.java.encryption.FakeCryptoManager;
import org.junit.jupiter.api.Test;

import static com.couchbase.client.core.util.CbCollections.setOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JsonObjectCryptoTest {
  @Test
  void encrypt() {

    JsonObject doc = JsonObject.create()
        .put("notSecret", 123);

    JsonObjectCrypto crypto = doc.crypto(new FakeCryptoManager());

    crypto.put("magicWord", "xyzzy");

    assertEquals(
        JsonObject.create()
            .put("alg", "FAKE")
            .put("ciphertext", "Inh5enp5Ig=="),
        doc.getObject("encrypted$magicWord"));

    assertEquals("xyzzy", crypto.getString("magicWord"));

    // unencrypted fields are invisible to the crypto lens
    assertNull(crypto.getInt("notSecret"));

    assertTrue(crypto.hasEncryptedField("magicWord"));
    assertFalse(crypto.hasEncryptedField("notSecret"));

    assertEquals(setOf("magicWord"), crypto.getEncryptedFieldNames());
    assertEquals(setOf("notSecret"), crypto.getUnencryptedFieldNames());
  }
}
