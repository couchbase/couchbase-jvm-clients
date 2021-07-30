package com.couchbase.client.java.encryption.databind.jackson;

import com.couchbase.client.core.encryption.CryptoManager;
import com.couchbase.client.java.encryption.FakeCryptoManager;
import com.couchbase.client.java.encryption.annotation.Encrypted;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.util.function.Consumer;

import static com.couchbase.client.java.encryption.annotation.Encrypted.Migration.FROM_UNENCRYPTED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

public abstract class AbstractEncryptionModuleTest {

  protected static final String jsonPlaintext = "{\n" +
      "  \"maxim\": \"The enemy knows the system.\"\n" +
      "}\n";

  protected static final String jsonEncrypted = "{\n" +
      "  \"encrypted$maxim\": {\n" +
      "    \"alg\": \"FAKE\",\n" +
      "    \"ciphertext\": \"IlRoZSBlbmVteSBrbm93cyB0aGUgc3lzdGVtLiI=\"\n" +
      "  }\n" +
      "}\n";

  protected static final String jsonMixed = "{\n" +
      "  \"greeting\":\"hello\",\n" +
      "  \"encrypted$maxim\": {\n" +
      "    \"alg\": \"FAKE\",\n" +
      "    \"ciphertext\": \"IlRoZSBlbmVteSBrbm93cyB0aGUgc3lzdGVtLiI=\"\n" +
      "  }\n" +
      "}\n";

  protected static CryptoManager getCryptoManager() {
    return new FakeCryptoManager();
  }

  @ParameterizedTest
  @ValueSource(classes = {
      AnnotatedField.class,
      AnnotatedGetter.class,
      AnnotatedSetter.class,
      AnnotatedGetterImmutable.class,
      AnnotatedFieldMeta.class,
      AnnotatedGetterMeta.class,
      AnnotatedSetterMeta.class,
      AnnotatedGetterImmutableMeta.class,
      AnnotatedGetterWithMigrationFromUnencrypted.class,
  })
  <T extends MaximHolder> void canSerializeAndDeserialize(Class<T> pojoClass) throws Exception {
    // delegate to subclass to use either standard or repackaged Jackson
    doCheck(pojoClass, jsonEncrypted, pojo -> assertEquals("The enemy knows the system.", pojo.getMaxim()));
  }

  @Test
  void worksIfThereAreNonSensitiveFields() throws Exception {
    doCheck(AlsoHasNonSensitiveFields.class, jsonMixed, pojo -> {
      assertEquals("The enemy knows the system.", pojo.getMaxim());
      assertEquals("hello", pojo.getGreeting());
    });
  }

  @Test
  void worksWhenMigratingFromUnencrypted() throws Exception {
    doCheck(AnnotatedGetterWithMigrationFromUnencrypted.class, jsonPlaintext, jsonEncrypted, pojo -> {
      assertEquals("The enemy knows the system.", pojo.getMaxim());
    });
  }

  @Test
  void failsOnUnencryptedFieldWithoutMigration() throws Exception {
    Exception e = assertThrows(Exception.class, () ->
        doCheck(AnnotatedGetter.class, jsonPlaintext, pojo -> {
        }));
    if (!e.getMessage().contains("Unrecognized field \"maxim\"")) {
      fail("unexpected exception message: " + e);
    }
  }

  protected <T extends MaximHolder> void doCheck(Class<T> pojoClass, String inputJson, Consumer<T> pojoValidator) throws Exception {
    // expected output is same as input
    doCheck(pojoClass, inputJson, inputJson, pojoValidator);
  }

  protected abstract <T extends MaximHolder> void doCheck(Class<T> pojoClass, String inputJson, String expectedOutputJson, Consumer<T> pojoValidator) throws Exception;

  protected interface MaximHolder {
    String getMaxim();
  }

  protected static class AnnotatedField implements MaximHolder {
    @Encrypted
    private String maxim;

    public String getMaxim() {
      return maxim;
    }

    @SuppressWarnings("unused")
    public void setMaxim(String maxim) {
      this.maxim = maxim;
    }
  }

  protected static class AnnotatedGetter implements MaximHolder {
    private String maxim;

    @Encrypted
    public String getMaxim() {
      return maxim;
    }

    @SuppressWarnings("unused")
    public void setMaxim(String maxim) {
      this.maxim = maxim;
    }
  }

  protected static class AnnotatedSetter implements MaximHolder {
    private String maxim;

    public String getMaxim() {
      return maxim;
    }

    @Encrypted
    @SuppressWarnings("unused")
    public void setMaxim(String maxim) {
      this.maxim = maxim;
    }
  }

  protected static class AnnotatedGetterImmutable implements MaximHolder {
    private final String maxim;

    public AnnotatedGetterImmutable(
        @com.fasterxml.jackson.annotation.JsonProperty("maxim")
        @com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonProperty("maxim")
            String maxim) {
      this.maxim = maxim;
    }

    @Encrypted
    public String getMaxim() {
      return maxim;
    }
  }

  /**
   * Applying this annotation to a property should have the same effect
   * as applying the {@link Encrypted} annotation.
   */
  @Encrypted
  @Retention(RetentionPolicy.RUNTIME)
  @interface Meta {
  }

  protected static class AnnotatedFieldMeta implements MaximHolder {
    @Meta
    private String maxim;

    public String getMaxim() {
      return maxim;
    }

    @SuppressWarnings("unused")
    public void setMaxim(String maxim) {
      this.maxim = maxim;
    }
  }

  protected static class AnnotatedGetterMeta implements MaximHolder {
    private String maxim;

    @Meta
    public String getMaxim() {
      return maxim;
    }

    @SuppressWarnings("unused")
    public void setMaxim(String maxim) {
      this.maxim = maxim;
    }
  }

  protected static class AnnotatedSetterMeta implements MaximHolder {
    private String maxim;

    public String getMaxim() {
      return maxim;
    }

    @Meta
    @SuppressWarnings("unused")
    public void setMaxim(String maxim) {
      this.maxim = maxim;
    }
  }

  protected static class AnnotatedGetterImmutableMeta implements MaximHolder {
    private final String maxim;

    public AnnotatedGetterImmutableMeta(
        @com.fasterxml.jackson.annotation.JsonProperty("maxim")
        @com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonProperty("maxim")
            String maxim) {
      this.maxim = maxim;
    }

    @Meta
    public String getMaxim() {
      return maxim;
    }
  }

  protected static class AlsoHasNonSensitiveFields implements MaximHolder {
    private String greeting;

    @Encrypted
    private String maxim;

    public String getMaxim() {
      return maxim;
    }

    @SuppressWarnings("unused")
    public void setMaxim(String maxim) {
      this.maxim = maxim;
    }

    public String getGreeting() {
      return greeting;
    }

    public void setGreeting(String greeting) {
      this.greeting = greeting;
    }
  }

  protected static class AnnotatedGetterWithMigrationFromUnencrypted implements MaximHolder {
    private String maxim;

    @Encrypted(migration = FROM_UNENCRYPTED)
    public String getMaxim() {
      return maxim;
    }

    @SuppressWarnings("unused")
    public void setMaxim(String maxim) {
      this.maxim = maxim;
    }
  }
}
