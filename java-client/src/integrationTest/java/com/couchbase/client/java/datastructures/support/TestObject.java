/*
 * Copyright (c) 2019 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.java.collections.support;

import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.Objects;
/* Used in several datastructures tests.
 * sort of a minimal example of what you need -
 *   - Default Constructor
 *   - Getters/Setters
 *   - We have one property we don't serialize, just for fun
 */

@JsonIgnoreProperties("ignoredBoolean")
public class TestObject {

    private int integer;
    private String string;

    public Boolean getIgnoredBoolean() {
        return ignoredBoolean;
    }

    public void setIgnoredBoolean(Boolean ignoredBoolean) {
        this.ignoredBoolean = ignoredBoolean;
    }

    private Boolean ignoredBoolean;

    public TestObject() {
        this.integer = 0;
        this.string = "";
        this.ignoredBoolean = false;
    }

    public TestObject(int integer, String string) {
        this.string = string;
        this.integer = integer;
        this.ignoredBoolean = false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TestObject testObject = (TestObject) o;
        return integer == testObject.integer &&
                Objects.equals(string, testObject.string);
    }

    @Override
    public int hashCode() {
        return Objects.hash(integer, string);
    }

    public int getInteger() {
        return integer;
    }

    public void setInteger(int integer) {
        this.integer = integer;
    }

    public String getString() {
        return string;
    }

    public void setString(String string) {
        this.string = string;
    }
}
