/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.pdx.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import javax.annotation.Nullable;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.pdx.FieldType;
import org.apache.geode.test.junit.categories.SerializationTest;

@Category(SerializationTest.class)
public class PdxFieldTest {
  static final String FIELD_NAME = "fieldName";
  static final int FIELD_INDEX = 13;
  static final int VAR_LEN_FIELD_SEQ_ID = 37;
  static final FieldType FIELD_TYPE = FieldType.OBJECT;
  static final boolean IDENTITY_FIELD = true;

  @Nullable
  static FieldType getAnotherFieldType(FieldType anythingButThis) {
    for (var fieldType : FieldType.values()) {
      if (!anythingButThis.equals(fieldType)) {
        return fieldType;
      }
    }
    return null;
  }

  @Test
  public void testNoArgConstructor() {
    final var emptyField = new PdxField();
    assertNull(emptyField.getFieldName());
    assertEquals(0, emptyField.getFieldIndex());
    assertEquals(0, emptyField.getVarLenFieldSeqId());
    assertNull(emptyField.getFieldType());
    assertFalse(emptyField.isIdentityField());
    try {
      assertEquals(false, emptyField.isVariableLengthType());
      fail();
    } catch (NullPointerException npe) {
      // Pass.
    }
    assertEquals(0, emptyField.getRelativeOffset());
    assertEquals(0, emptyField.getVlfOffsetIndex());
    assertEquals(false, emptyField.isDeleted());
    try {
      assertNull(emptyField.getTypeIdString());
      fail();
    } catch (NullPointerException npe) {
      // Pass.
    }
  }

  @Test
  public void testSomeArgConstructor() {
    final var nonEmptyField =
        new PdxField(FIELD_NAME, FIELD_INDEX, VAR_LEN_FIELD_SEQ_ID, FIELD_TYPE, IDENTITY_FIELD);
    assertEquals(FIELD_NAME, nonEmptyField.getFieldName());
    assertEquals(FIELD_INDEX, nonEmptyField.getFieldIndex());
    assertEquals(VAR_LEN_FIELD_SEQ_ID, nonEmptyField.getVarLenFieldSeqId());
    assertEquals(FIELD_TYPE, nonEmptyField.getFieldType());
    assertEquals(IDENTITY_FIELD, nonEmptyField.isIdentityField());
    assertEquals(!FIELD_TYPE.isFixedWidth(), nonEmptyField.isVariableLengthType());
    assertEquals(0, nonEmptyField.getRelativeOffset());
    assertEquals(0, nonEmptyField.getVlfOffsetIndex());
    assertEquals(false, nonEmptyField.isDeleted());
    assertEquals(FIELD_TYPE.toString(), nonEmptyField.getTypeIdString());
  }

  @Test
  public void testCompareTo() {
    final var field =
        new PdxField(FIELD_NAME, FIELD_INDEX, VAR_LEN_FIELD_SEQ_ID, FIELD_TYPE, IDENTITY_FIELD);
    assertEquals(0, field.compareTo(field));

    final var sameFieldNameOnly = new PdxField(FIELD_NAME, FIELD_INDEX + 1,
        VAR_LEN_FIELD_SEQ_ID + 1, getAnotherFieldType(FIELD_TYPE), !IDENTITY_FIELD);
    assertEquals(0, field.compareTo(sameFieldNameOnly));

    final var differentFieldNameOnly = new PdxField("Not " + FIELD_NAME, FIELD_INDEX,
        VAR_LEN_FIELD_SEQ_ID, FIELD_TYPE, IDENTITY_FIELD);
    assertNotEquals(0, field.compareTo(differentFieldNameOnly));
  }

  @Test
  public void testHashCode() {
    final var field =
        new PdxField(FIELD_NAME, FIELD_INDEX, VAR_LEN_FIELD_SEQ_ID, FIELD_TYPE, IDENTITY_FIELD);
    final var sameFieldNameAndFieldType = new PdxField(FIELD_NAME, FIELD_INDEX + 1,
        VAR_LEN_FIELD_SEQ_ID + 1, FIELD_TYPE, !IDENTITY_FIELD);
    assertEquals(field.hashCode(), sameFieldNameAndFieldType.hashCode());

    final var differentFieldName = new PdxField("Not " + FIELD_NAME, FIELD_INDEX + 1,
        VAR_LEN_FIELD_SEQ_ID + 1, FIELD_TYPE, !IDENTITY_FIELD);
    assertNotEquals(field.hashCode(), differentFieldName.hashCode());

    final var differentFieldType = new PdxField(FIELD_NAME, FIELD_INDEX + 1,
        VAR_LEN_FIELD_SEQ_ID + 1, getAnotherFieldType(FIELD_TYPE), !IDENTITY_FIELD);
    assertNotEquals(field.hashCode(), differentFieldType.hashCode());
  }

  @Test
  public void testEquals() {
    final var field =
        new PdxField(FIELD_NAME, FIELD_INDEX, VAR_LEN_FIELD_SEQ_ID, FIELD_TYPE, IDENTITY_FIELD);
    field.setDeleted(true);
    assertTrue(field.equals(field));
    assertFalse(field.equals(null));
    assertFalse(field.equals(new Object()));

    final var sameFieldNameFieldTypeAndDeleted = new PdxField(FIELD_NAME, FIELD_INDEX + 1,
        VAR_LEN_FIELD_SEQ_ID + 1, FIELD_TYPE, !IDENTITY_FIELD);
    sameFieldNameFieldTypeAndDeleted.setDeleted(true);
    assertTrue(field.equals(sameFieldNameFieldTypeAndDeleted));

    final var differentFieldName = new PdxField("Not " + FIELD_NAME, FIELD_INDEX + 1,
        VAR_LEN_FIELD_SEQ_ID + 1, FIELD_TYPE, !IDENTITY_FIELD);
    differentFieldName.setDeleted(true);
    assertFalse(field.equals(differentFieldName));

    final var differentFieldType = new PdxField(FIELD_NAME, FIELD_INDEX + 1,
        VAR_LEN_FIELD_SEQ_ID + 1, getAnotherFieldType(FIELD_TYPE), !IDENTITY_FIELD);
    differentFieldType.setDeleted(true);
    assertFalse(field.equals(differentFieldType));

    final var differentDeleted = new PdxField(FIELD_NAME, FIELD_INDEX + 1,
        VAR_LEN_FIELD_SEQ_ID + 1, FIELD_TYPE, !IDENTITY_FIELD);
    differentDeleted.setDeleted(false);
    assertFalse(field.equals(differentDeleted));
  }

  @Test
  public void testToString() {
    final var field =
        new PdxField(FIELD_NAME, FIELD_INDEX, VAR_LEN_FIELD_SEQ_ID, FIELD_TYPE, IDENTITY_FIELD);
    assertEquals(0, field.toString().indexOf(FIELD_NAME));
  }

  @Test
  public void testToStream() {
    final var field =
        new PdxField(FIELD_NAME, FIELD_INDEX, VAR_LEN_FIELD_SEQ_ID, FIELD_TYPE, IDENTITY_FIELD);
    var byteArrayOutputStream = new ByteArrayOutputStream();
    field.toStream(new PrintStream(byteArrayOutputStream));
    assertNotEquals(-1, byteArrayOutputStream.toString().indexOf(FIELD_NAME));
  }

  @Test
  public void testToDataAndFromData() throws IOException, ClassNotFoundException {
    final var before =
        new PdxField(FIELD_NAME, FIELD_INDEX, VAR_LEN_FIELD_SEQ_ID, FIELD_TYPE, IDENTITY_FIELD);
    var byteArrayOutputStream = new ByteArrayOutputStream(1024);
    var dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    before.toData(dataOutputStream);
    dataOutputStream.close();

    final var after = new PdxField();
    var byteArrayInputStream =
        new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
    var dataInputStream = new DataInputStream(byteArrayInputStream);
    after.fromData(dataInputStream);

    assertEquals(before.getFieldName(), after.getFieldName());
    assertEquals(before.getFieldIndex(), after.getFieldIndex());
    assertEquals(before.getVarLenFieldSeqId(), after.getVarLenFieldSeqId());
    assertEquals(before.getFieldType(), after.getFieldType());
    assertEquals(before.isIdentityField(), after.isIdentityField());
    assertEquals(before.isVariableLengthType(), after.isVariableLengthType());
    assertEquals(before.getRelativeOffset(), after.getRelativeOffset());
    assertEquals(before.getVlfOffsetIndex(), after.getVlfOffsetIndex());
    assertEquals(before.isDeleted(), after.isDeleted());
    assertEquals(before.getTypeIdString(), after.getTypeIdString());
  }
}
