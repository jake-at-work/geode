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
package org.apache.geode.cache.lucene;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;

import org.apache.lucene.index.IndexableField;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.lucene.internal.repository.serializer.SerializerTestHelper;
import org.apache.geode.cache.lucene.test.Customer;
import org.apache.geode.cache.lucene.test.GrandSubCustomer;
import org.apache.geode.cache.lucene.test.Page;
import org.apache.geode.cache.lucene.test.Person;
import org.apache.geode.test.junit.categories.LuceneTest;

@Category({LuceneTest.class})
public class FlatFormatSerializerJUnitTest {

  private HashSet<Person> createCollectionObjectContacts() {
    var contact1 = new Person("Tommi Jackson", new String[] {"5036330001", "5036330002"}, 1);
    var contact2 = new Person("Tommi2 Skywalker", new String[] {"5036330003", "5036330004"}, 2);
    HashSet<Person> contacts1 = new HashSet();
    contacts1.add(contact1);
    contacts1.add(contact2);
    return contacts1;
  }

  @Test
  public void verifyFieldCountsInDocument() {
    var fields = new String[] {"name", "phoneNumbers", "myHomePages.content", "contacts.name",
        "contacts.email", "contacts.phoneNumbers", "contacts.address", "contacts.revenue",
        "contacts.homepage.id", "contacts.homepage.title", "contacts.homepage.content",
        LuceneService.REGION_VALUE_FIELD};

    var serializer = new FlatFormatSerializer();

    var contact1 = new Person("Tommi Jackson", new String[] {"5036330001", "5036330002"}, 1);
    var contact2 = new Person("Tommi2 Skywalker", new String[] {"5036330003", "5036330004"}, 2);
    HashSet<Person> contacts1 = new HashSet();
    contacts1.add(contact1);
    contacts1.add(contact2);
    ArrayList<String> phoneNumbers = new ArrayList();
    phoneNumbers.add("5035330001");
    phoneNumbers.add("5035330002");
    var myHomePages1 = new Page[] {new Page(131), new Page(132)};
    var customer = new Customer("Tommy Jackson", phoneNumbers, contacts1, myHomePages1);
    var doc1 = SerializerTestHelper.invokeSerializer(serializer, customer, fields);
    assertEquals(23, doc1.getFields().size());
    assertEquals("Tommy Jackson", doc1.getField("name").stringValue());
  }

  @Test
  public void shouldQueryOnFieldInArrayObject() {
    var fields = new String[] {"myHomePages.content"};

    var serializer = new FlatFormatSerializer();

    var myHomePages1 = new Page[] {new Page(131), new Page(132)};
    var customer = new Customer("Tommy Jackson", null, null, myHomePages1);
    var doc1 = SerializerTestHelper.invokeSerializer(serializer, customer, fields);

    var fieldsInDoc = doc1.getFields("myHomePages.content");
    var results = getResultCollection(fieldsInDoc, false);
    assertEquals(2, results.size());
    assertTrue(results.contains("Hello world no 131"));
    assertTrue(results.contains("Hello world no 132"));
  }

  @Test
  public void shouldIndexOnInheritedFields() {
    var fields = new String[] {"myHomePages.content"};

    var serializer = new FlatFormatSerializer();

    var myHomePages1 = new Page[] {new Page(131), new Page(132)};
    var customer = new GrandSubCustomer("Tommy Jackson", null, null, myHomePages1);
    var doc1 = SerializerTestHelper.invokeSerializer(serializer, customer, fields);

    var fieldsInDoc = doc1.getFields("myHomePages.content");
    var results = getResultCollection(fieldsInDoc, false);
    assertEquals(2, results.size());
    var value = results.iterator().next();
    assertTrue(results.contains("Hello world no 131"));
    assertTrue(results.contains("Hello world no 132"));
  }

  @Test
  public void shouldQueryOnFieldInCollectionObject() {
    var fields = new String[] {"contacts.name"};

    var serializer = new FlatFormatSerializer();

    var contacts1 = createCollectionObjectContacts();
    var customer = new Customer("Tommy Jackson", null, contacts1, null);
    var doc1 = SerializerTestHelper.invokeSerializer(serializer, customer, fields);

    var fieldsInDoc = doc1.getFields("contacts.name");
    var results = getResultCollection(fieldsInDoc, false);
    assertEquals(2, results.size());
    assertTrue(results.contains("Tommi Jackson"));
    assertTrue(results.contains("Tommi2 Skywalker"));
  }

  @Test
  public void shouldQueryOnFieldWithAnalyzerInCollectionObject() {
    var fields = new String[] {"contacts.email"};

    var serializer = new FlatFormatSerializer();

    var contacts1 = createCollectionObjectContacts();
    var customer = new Customer("Tommy Jackson", null, contacts1, null);
    var doc1 = SerializerTestHelper.invokeSerializer(serializer, customer, fields);

    var fieldsInDoc = doc1.getFields("contacts.email");
    var results = getResultCollection(fieldsInDoc, false);
    assertEquals(2, results.size());
    assertTrue(results.contains("Tommi.Jackson@pivotal.io"));
    assertTrue(results.contains("Tommi2.Skywalker@pivotal.io"));
  }

  @Test
  public void shouldQueryOnIntFieldInCollectionObject() {
    var fields = new String[] {"contacts.revenue"};

    var serializer = new FlatFormatSerializer();

    var contacts1 = createCollectionObjectContacts();
    var customer = new Customer("Tommy Jackson", null, contacts1, null);
    var doc1 = SerializerTestHelper.invokeSerializer(serializer, customer, fields);

    var fieldsInDoc = doc1.getFields("contacts.revenue");
    var intResults = getResultCollection(fieldsInDoc, true);
    assertEquals(2, intResults.size());
    assertTrue(intResults.contains(100));
    assertTrue(intResults.contains(200));
  }

  @Test
  public void shouldQueryOnFieldInThirdLevelObject() {
    var fields = new String[] {"contacts.homepage.title"};

    var serializer = new FlatFormatSerializer();

    var contacts1 = createCollectionObjectContacts();
    var customer = new Customer("Tommy Jackson", null, contacts1, null);
    var doc1 = SerializerTestHelper.invokeSerializer(serializer, customer, fields);

    var fieldsInDoc = doc1.getFields("contacts.homepage.title");
    var results = getResultCollection(fieldsInDoc, false);
    assertEquals(2, results.size());
    assertTrue(results.contains("developer"));
    assertTrue(results.contains("manager"));
  }

  @Test
  public void shouldParseRegionValueFieldForString() {
    var fields = new String[] {"name", "contacts.name", "contacts.email", "contacts.revenue",
        "contacts.address", "contacts.homepage.id", "contacts.homepage.title",
        "contacts.homepage.content", LuceneService.REGION_VALUE_FIELD};

    var serializer = new FlatFormatSerializer();
    var str = "Hello world";
    var doc1 = SerializerTestHelper.invokeSerializer(serializer, str, fields);
    assertEquals(1, doc1.getFields().size());
    assertEquals("Hello world", doc1.getField(LuceneService.REGION_VALUE_FIELD).stringValue());
  }

  @Test
  public void shouldParseRegionValueFieldForInteger() {
    var fields = new String[] {"name", "contacts.name", "contacts.email", "contacts.revenue",
        "contacts.address", "contacts.homepage.id", "contacts.homepage.title",
        "contacts.homepage.content", LuceneService.REGION_VALUE_FIELD};

    var serializer = new FlatFormatSerializer();
    Integer integer = 15;
    var doc1 = SerializerTestHelper.invokeSerializer(serializer, integer, fields);
    assertEquals(1, doc1.getFields().size());
    assertEquals(15, doc1.getField(LuceneService.REGION_VALUE_FIELD).numericValue());
  }

  @Test
  public void shouldNotParseNestedObjectWithoutFields() {
    var fields =
        new String[] {"name", "contacts", "contacts.homepage", "contacts.missing", "missing2"};

    var serializer = new FlatFormatSerializer();
    HashSet<Person> contacts1 = new HashSet();
    var contact1 = new Person("Tommi Jackson", new String[] {"5036330001", "5036330002"}, 1);
    var contact2 = new Person("Tommi2 Skywalker", new String[] {"5036330003", "5036330004"}, 2);
    contacts1.add(contact1);
    contacts1.add(contact2);
    ArrayList<String> phoneNumbers = new ArrayList();
    phoneNumbers.add("5035330001");
    phoneNumbers.add("5035330002");
    var myHomePages1 = new Page[] {new Page(131), new Page(132)};
    var customer = new Customer("Tommy Jackson", phoneNumbers, contacts1, myHomePages1);
    var doc1 = SerializerTestHelper.invokeSerializer(serializer, customer, fields);
    assertEquals(1, doc1.getFields().size());
    assertEquals("Tommy Jackson", doc1.getField("name").stringValue());
  }

  private Collection<Object> getResultCollection(IndexableField[] fieldsInDoc, boolean isNumeric) {
    Collection<Object> results = new LinkedHashSet();
    for (var field : fieldsInDoc) {
      if (isNumeric) {
        results.add(field.numericValue());
      } else {
        results.add(field.stringValue());
      }
    }
    return results;
  }
}
