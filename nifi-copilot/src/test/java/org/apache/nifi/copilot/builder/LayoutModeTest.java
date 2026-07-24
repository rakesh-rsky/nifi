/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.copilot.builder;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.*;

class LayoutModeTest {

    @Test
    void nullReturnsEngine() {
        assertEquals(LayoutMode.ENGINE, LayoutMode.fromString(null));
    }

    @Test
    void blankReturnsEngine() {
        assertEquals(LayoutMode.ENGINE, LayoutMode.fromString("   "));
    }

    @ParameterizedTest
    @ValueSource(strings = {"engine", "ENGINE", "Engine"})
    void engineVariantsReturnEngine(final String value) {
        assertEquals(LayoutMode.ENGINE, LayoutMode.fromString(value));
    }

    @ParameterizedTest
    @ValueSource(strings = {"disabled", "DISABLED", "Disabled"})
    void disabledVariantsReturnDisabled(final String value) {
        assertEquals(LayoutMode.DISABLED, LayoutMode.fromString(value));
    }

    @Test
    void invalidValueThrowsIllegalArgumentException() {
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class, () -> LayoutMode.fromString("invalid"));
        assertTrue(ex.getMessage().contains("invalid"));
        assertTrue(ex.getMessage().contains("engine"));
        assertTrue(ex.getMessage().contains("disabled"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"auto", "full", "incremental", "on", "off"})
    void unknownValuesAreRejected(final String value) {
        assertThrows(IllegalArgumentException.class, () -> LayoutMode.fromString(value));
    }
}
