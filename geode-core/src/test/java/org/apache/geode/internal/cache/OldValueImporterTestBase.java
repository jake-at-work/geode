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
package org.apache.geode.internal.cache;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Test;

import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.cache.EntryEventImpl.OldValueImporter;

public abstract class OldValueImporterTestBase {

  protected abstract OldValueImporter createImporter();

  protected abstract Object getOldValueFromImporter(OldValueImporter ovi);

  protected abstract void toData(OldValueImporter ovi, HeapDataOutputStream hdos)
      throws IOException;

  protected abstract void fromData(OldValueImporter ovi, byte[] bytes)
      throws IOException, ClassNotFoundException;

  @Test
  public void testValueSerialization() throws Exception {
    byte[] bytes = new byte[1024];
    HeapDataOutputStream hdos = new HeapDataOutputStream(bytes);
    OldValueImporter imsg = createImporter();

    // null byte array value
    {
      OldValueImporter omsg = createImporter();
      omsg.importOldBytes(null, false);
      toData(omsg, hdos);
      fromData(imsg, bytes);
      assertEquals(null, getOldValueFromImporter(imsg));
    }

    // null object value
    {
      OldValueImporter omsg = createImporter();
      omsg.importOldObject(null, true);
      toData(omsg, hdos);
      fromData(imsg, bytes);
      assertEquals(null, getOldValueFromImporter(imsg));
    }

    // simple byte array
    {
      byte[] baValue = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9};
      OldValueImporter omsg = createImporter();
      omsg.importOldBytes(baValue, false);
      hdos = new HeapDataOutputStream(bytes);
      toData(omsg, hdos);
      fromData(imsg, bytes);
      assertArrayEquals(baValue, (byte[]) getOldValueFromImporter(imsg));
    }

    // String in serialized form
    {
      String stringValue = "1,2,3,4,5,6,7,8,9";
      byte[] stringValueBlob = EntryEventImpl.serialize(stringValue);
      OldValueImporter omsg = createImporter();
      omsg.importOldBytes(stringValueBlob, true);
      hdos = new HeapDataOutputStream(bytes);
      toData(omsg, hdos);
      fromData(imsg, bytes);
      assertArrayEquals(stringValueBlob,
          ((VMCachedDeserializable) getOldValueFromImporter(imsg)).getSerializedValue());
    }

    // String in object form
    {
      String stringValue = "1,2,3,4,5,6,7,8,9";
      byte[] stringValueBlob = EntryEventImpl.serialize(stringValue);
      OldValueImporter omsg = createImporter();
      omsg.importOldObject(stringValue, true);
      hdos = new HeapDataOutputStream(bytes);
      toData(omsg, hdos);
      fromData(imsg, bytes);
      assertArrayEquals(stringValueBlob,
          ((VMCachedDeserializable) getOldValueFromImporter(imsg)).getSerializedValue());
    }
  }
}
