/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
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
package org.apache.geode.cache.lucene.internal.repository.serializer;

import static org.apache.geode.cache.lucene.internal.repository.serializer.SerializerTestHelper.invokeSerializer;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.LuceneTest;


/**
 * Unit test of the ReflectionFieldMapperClass. Tests that all field types are mapped correctly.
 */
@Category({LuceneTest.class})
public class ReflectionFieldMapperJUnitTest {

  @Test
  public void testAllFields() {

    var allFields = new String[] {"s", "i", "l", "d", "f", "s2"};
    var mapper1 = new ReflectionLuceneSerializer(Type1.class, allFields);
    var mapper2 = new ReflectionLuceneSerializer(Type2.class, allFields);

    var type1 = new Type1("a", 1, 2L, 3.0, 4.0f);
    var type2 = new Type2("a", 1, 2L, 3.0, 4.0f, "b");

    var doc1 = invokeSerializer(mapper1, type1, allFields);

    assertEquals(5, doc1.getFields().size());
    assertEquals("a", doc1.getField("s").stringValue());
    assertEquals(1, doc1.getField("i").numericValue());
    assertEquals(2L, doc1.getField("l").numericValue());
    assertEquals(3.0, doc1.getField("d").numericValue());
    assertEquals(4.0f, doc1.getField("f").numericValue());

    var doc2 = invokeSerializer(mapper2, type2, allFields);

    assertEquals(6, doc2.getFields().size());
    assertEquals("a", doc2.getField("s").stringValue());
    assertEquals("b", doc2.getField("s2").stringValue());
    assertEquals(1, doc2.getField("i").numericValue());
    assertEquals(2L, doc2.getField("l").numericValue());
    assertEquals(3.0, doc2.getField("d").numericValue());
    assertEquals(4.0f, doc2.getField("f").numericValue());
  }

  @Test
  public void testIgnoreInvalid() {

    var fields = new String[] {"s", "o", "s2"};
    var mapper = new ReflectionLuceneSerializer(Type2.class, fields);

    var type2 = new Type2("a", 1, 2L, 3.0, 4.0f, "b");

    var doc = invokeSerializer(mapper, type2, fields);

    assertEquals(2, doc.getFields().size());
    assertEquals("a", doc.getField("s").stringValue());
    assertEquals("b", doc.getField("s2").stringValue());
  }

  @Test
  public void testNullField() {

    var fields = new String[] {"s", "o", "s2"};
    var mapper = new ReflectionLuceneSerializer(Type2.class, fields);

    var type2 = new Type2("a", 1, 2L, 3.0, 4.0f, null);

    var doc = invokeSerializer(mapper, type2, fields);

    assertEquals(1, doc.getFields().size());
    assertEquals("a", doc.getField("s").stringValue());
    assertNull(doc.getField("s2"));
  }
}
