package com.couchbase.client.java.encryption.databind.jackson;

import com.couchbase.client.core.encryption.CryptoManager;
import com.couchbase.client.java.encryption.FakeCryptoManager;
import com.couchbase.client.java.encryption.annotation.EncryptedField;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;

public abstract class AbstractEncryptionModuleTest {

  protected static final String json = "{\n" +
      "  \"encrypted$maxim\": {\n" +
      "    \"alg\": \"FAKE\",\n" +
      "    \"ciphertext\": \"IlRoZSBlbmVteSBrbm93cyB0aGUgc3lzdGVtLiI=\"\n" +
      "  }\n" +
      "}\n";

  protected static final String jsonWithUnencrypted = "{\n" +
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
  })
  <T extends MaximHolder> void canSerializeAndDeserialize(Class<T> pojoClass) throws Exception {
    // delegate to subclass to use either standard or repackaged Jackson
    doCheck(pojoClass, json, pojo -> assertEquals("The enemy knows the system.", pojo.getMaxim()));
  }

  @Test
  void worksIfThereAreNonSensitiveFields() throws Exception {
    doCheck(AlsoHasNonSensitiveFields.class, jsonWithUnencrypted, pojo -> {
      assertEquals("The enemy knows the system.", pojo.getMaxim());
      assertEquals("hello", pojo.getGreeting());
    });
  }

  protected abstract <T extends MaximHolder> void doCheck(Class<T> pojoClass, String json, Consumer<T> pojoValidator) throws Exception;

  protected interface MaximHolder {
    String getMaxim();
  }

  protected static class AnnotatedField implements MaximHolder {
    @EncryptedField
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

    @EncryptedField
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

    @EncryptedField
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

    @EncryptedField
    public String getMaxim() {
      return maxim;
    }
  }

  /**
   * Applying this annotation to a property should have the same effect
   * as applying the {@link EncryptedField} annotation.
   */
  @EncryptedField
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

    @EncryptedField
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

}
