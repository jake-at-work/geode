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
package org.apache.geode.pdx;

import static org.apache.geode.distributed.ConfigurationProperties.DISTRIBUTED_SYSTEM_ID;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Properties;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.pdx.internal.json.PdxToJSON;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.RestAPITest;
import org.apache.geode.test.util.ResourceUtils;

@Category({RestAPITest.class})
public class JSONPdxClientServerDUnitTest extends JUnit4CacheTestCase {

  @Override
  public final void preTearDownCacheTestCase() {
    // this test creates client caches in some VMs and so
    // breaks the contract of CacheTestCase to hold caches in
    // that class's "cache" instance variable
    disconnectAllFromDS();
  }

  @Test
  public void testSimplePut() {
    var host = Host.getHost(0);
    var vm0 = host.getVM(0);
    var vm1 = host.getVM(1);
    var vm2 = host.getVM(2);
    var vm3 = host.getVM(3);


    createServerRegion(vm0);
    var port = createServerRegion(vm3);
    createClientRegion(vm1, port);
    createClientRegion(vm2, port);

    vm1.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        JSONAllStringTest();
        return null;
      }
    });

    vm2.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        JSONAllByteArrayTest();
        return null;
      }
    });
  }

  @Test
  public void testSimplePutWithSortedJSONField() {
    var host = Host.getHost(0);
    var vm0 = host.getVM(0);
    var vm1 = host.getVM(1);
    var vm2 = host.getVM(2);
    var vm3 = host.getVM(3);


    createServerRegion(vm0);
    var port = createServerRegion(vm3);
    createClientRegion(vm1, port);
    createClientRegion(vm2, port);

    vm1.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        try {
          System.setProperty(JSONFormatter.SORT_JSON_FIELD_NAMES_PROPERTY, "true");
          JSONAllStringTest();
        } finally {
          System.setProperty(JSONFormatter.SORT_JSON_FIELD_NAMES_PROPERTY, "false");
        }

        return null;
      }
    });

    vm2.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        try {
          System.setProperty(JSONFormatter.SORT_JSON_FIELD_NAMES_PROPERTY, "true");
          JSONAllByteArrayTest();
        } finally {
          System.setProperty(JSONFormatter.SORT_JSON_FIELD_NAMES_PROPERTY, "false");
        }
        return null;
      }
    });
  }

  // this is for unquote fielnames in json string
  @Test
  public void testSimplePut2() {
    var host = Host.getHost(0);
    var vm0 = host.getVM(0);
    var vm1 = host.getVM(1);
    var vm2 = host.getVM(2);
    var vm3 = host.getVM(3);


    createServerRegion(vm0);
    var port = createServerRegion(vm3);
    createClientRegion(vm1, port);
    createClientRegion(vm2, port);

    vm1.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        JSONUnQuoteFields();
        return null;
      }
    });

  }

  @Test
  public void testPdxInstanceAndJSONConversion() {
    var host = Host.getHost(0);
    var vm0 = host.getVM(0);
    var vm1 = host.getVM(1);
    var vm2 = host.getVM(2);
    var vm3 = host.getVM(3);

    createServerRegion(vm0, true);
    var port = createServerRegion(vm3, true);
    createClientRegion(vm1, port, true);
    createClientRegion(vm2, port, true);

    vm1.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        VerifyPdxInstanceAndJsonConversion();
        return null;
      }
    });
  }

  public void VerifyPdxInstanceAndJsonConversion() throws Exception {
    Region region = getRootRegion("testSimplePdx");

    // Create Object and initialize its members.
    var testObject = new TestObjectForJSONFormatter();
    testObject.defaultInitialization();

    // put the object into cache.
    region.put("101", testObject);

    // Get the object as PdxInstance
    var result = region.get("101");
    assertTrue(result instanceof PdxInstance);
    var pi = (PdxInstance) result;
    var json = JSONFormatter.toJSON(pi);

    JSONFormatVerifyUtility.verifyJsonWithJavaObject(json, testObject);

    // TestCase-2 : Validate Java-->JSON-->PdxInstance --> Java Mapping
    var actualTestObject = new TestObjectForJSONFormatter();
    actualTestObject.defaultInitialization();
    var objectMapper = new ObjectMapper();
    objectMapper.setDateFormat(new SimpleDateFormat("MM/dd/yyyy"));
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    validateReceivedJSON(region, actualTestObject, objectMapper);


  }

  private void validateReceivedJSON(Region region, TestObjectForJSONFormatter actualTestObject,
      ObjectMapper objectMapper) throws Exception {
    // 1. get the json from the object using Jackson Object Mapper
    var json = objectMapper.writeValueAsString(actualTestObject);
    var jsonWithClassType = actualTestObject.addClassTypeToJson(json);

    // 2. convert json into the PdxInstance and put it into the region
    var pi = JSONFormatter.fromJSON(jsonWithClassType);
    region.put("201", pi);

    // 3. get the value on key "201" and validate PdxInstance.getObject() API.
    var receivedObject = region.get("201");
    assertTrue(receivedObject instanceof PdxInstance);
    var receivedPdxInstance = (PdxInstance) receivedObject;

    // 4. get the actualType testObject from the pdxInstance and compare it with
    // actualTestObject
    var getObj = receivedPdxInstance.getObject();

    assertTrue(getObj instanceof TestObjectForJSONFormatter);

    var receivedTestObject = (TestObjectForJSONFormatter) getObj;

    assertEquals(actualTestObject, receivedTestObject);
  }

  String getJSONDir(String file) {
    var path = ResourceUtils.getResource(getClass(), file).getPath();
    return new File(path).getParent();
  }

  public void JSONUnQuoteFields() {
    System.setProperty("pdxToJson.unqouteFieldNames", "true");
    PdxToJSON.PDXTOJJSON_UNQUOTEFIELDNAMES = true;
    var jsonStringsDir =
        getJSONDir("/org/apache/geode/pdx/jsonStrings/unquoteJsonStrings/json1.txt");
    JSONAllStringTest(jsonStringsDir);
    PdxToJSON.PDXTOJJSON_UNQUOTEFIELDNAMES = false;
  }

  public void JSONAllStringTest() {
    var jsonStringsDir = getJSONDir("jsonStrings/json1.txt");
    JSONAllStringTest(jsonStringsDir);
  }

  public void JSONAllStringTest(String dirname) {

    var allJsons = loadAllJSON(dirname);
    var i = 0;
    for (var jsonData : allJsons) {
      if (jsonData != null) {
        i++;
        VerifyJSONString(jsonData);
      }
    }
    Assert.assertTrue(i >= 1, "Number of files should be more than 10 : " + i);
  }

  public void JSONAllByteArrayTest() {
    var jsonStringsDir = getJSONDir("jsonStrings/json1.txt");

    var allJsons = loadAllJSON(jsonStringsDir);
    var i = 0;
    for (var jsonData : allJsons) {
      if (jsonData != null) {
        i++;
        VerifyJSONByteArray(jsonData);
      }
    }
    Assert.assertTrue(i > 10, "Number of files should be more than 10");
  }

  static class JSONData {
    String jsonFileName;
    byte[] jsonByteArray;

    public JSONData(String fn, byte[] js) {
      jsonFileName = fn;
      jsonByteArray = js;
    }

    public String getFileName() {
      return jsonFileName;
    }

    public String getJsonString() {
      return new String(jsonByteArray);
    }

    public byte[] getJsonByteArray() {
      return jsonByteArray;
    }
  }


  public void VerifyJSONString(JSONData jd) {
    Region r = getRootRegion("testSimplePdx");

    var pdx = JSONFormatter.fromJSON(jd.getJsonString());

    r.put(1, pdx);

    pdx = (PdxInstance) r.get(1);

    var getJsonString = JSONFormatter.toJSON(pdx);

    var o1 = jsonParse(jd.getJsonString());
    var o2 = jsonParse(getJsonString);
    if (!Boolean.getBoolean(JSONFormatter.SORT_JSON_FIELD_NAMES_PROPERTY)) {
      assertEquals("Json Strings are not equal " + jd.getFileName() + " "
          + Boolean.getBoolean("pdxToJson.unqouteFieldNames"), o1, o2);
    } else {
      // we just need to compare length as blob will be different because fields are sorted
      assertEquals("Json Strings are not equal " + jd.getFileName() + " "
          + Boolean.getBoolean("pdxToJson.unqouteFieldNames"), o1.length(), o2.length());
    }

    var pdx2 = JSONFormatter.fromJSON(getJsonString);

    assertEquals("Pdx are not equal; json filename " + jd.getFileName(), pdx, pdx2);
  }

  protected static final int INT_TAB = '\t';
  protected static final int INT_LF = '\n';
  protected static final int INT_CR = '\r';
  protected static final int INT_SPACE = 0x0020;

  public String jsonParse(String jsonSting) {

    var ba = jsonSting.getBytes();
    var withoutspace = new byte[ba.length];

    var i = 0;
    var j = 0;
    for (i = 0; i < ba.length; i++) {
      int cbyte = ba[i];

      if (cbyte == INT_TAB || cbyte == INT_LF || cbyte == INT_CR || cbyte == INT_SPACE) {
        continue;
      }
      withoutspace[j++] = ba[i];
    }

    return new String(withoutspace, 0, j);

  }

  public void VerifyJSONByteArray(JSONData jd) {
    Region r = getRootRegion("testSimplePdx");

    var pdx = JSONFormatter.fromJSON(jd.getJsonByteArray());

    r.put(1, pdx);

    pdx = (PdxInstance) r.get(1);

    var jsonByteArray = JSONFormatter.toJSONByteArray(pdx);

    var o1 = jsonParse(jd.getJsonByteArray());
    var o2 = jsonParse(jsonByteArray);

    compareByteArray(o1, o2);

    var pdx2 = JSONFormatter.fromJSON(jsonByteArray);
    var pdxequals = pdx.equals(pdx2);

    assertEquals("Pdx are not equal for byte array ; json filename " + jd.getFileName(), pdx, pdx2);
  }

  public void compareByteArray(byte[] b1, byte[] b2) {
    if (b1.length != b2.length) {
      throw new IllegalStateException(
          "Json byte array length are not equal " + b1.length + " ; " + b2.length);
    }

    if (Boolean.getBoolean(JSONFormatter.SORT_JSON_FIELD_NAMES_PROPERTY)) {
      return;// we just need to compare length as blob will be different because fields are sorted
    }

    for (var i = 0; i < b1.length; i++) {
      if (b1[i] != b2[i]) {
        throw new IllegalStateException("Json byte arrays are not equal ");
      }
    }
  }

  public byte[] jsonParse(byte[] jsonBA) {

    var ba = jsonBA;
    var withoutspace = new byte[ba.length];

    var i = 0;
    var j = 0;
    for (i = 0; i < ba.length; i++) {
      int cbyte = ba[i];

      if (cbyte == INT_TAB || cbyte == INT_LF || cbyte == INT_CR || cbyte == INT_SPACE) {
        continue;
      }
      withoutspace[j++] = ba[i];
    }

    var retBA = new byte[j];

    for (i = 0; i < j; i++) {
      retBA[i] = withoutspace[i];
    }

    return retBA;

  }

  public static JSONData[] loadAllJSON(String jsondir) {
    var dir = new File(jsondir);

    var JSONDatas = new JSONData[dir.list().length];
    var i = 0;
    for (var jsonFileName : dir.list()) {

      if (!jsonFileName.contains(".txt")) {
        continue;
      }
      try {
        var ba = getBytesFromFile(dir.getAbsolutePath() + File.separator + jsonFileName);
        JSONDatas[i++] = new JSONData(jsonFileName, ba);
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    return JSONDatas;
  }

  public static byte[] getBytesFromFile(String fileName) throws IOException {
    var file = new File(fileName);

    java.io.InputStream is = new FileInputStream(file);

    // Get the size of the file
    var length = file.length();

    // Create the byte array to hold the data
    var bytes = new byte[(int) length];

    // Read in the bytes
    var offset = 0;
    var numRead = 0;
    while (offset < bytes.length
        && (numRead = is.read(bytes, offset, bytes.length - offset)) >= 0) {
      offset += numRead;
    }

    // Ensure all the bytes have been read in
    if (offset < bytes.length) {
      throw new IOException("Could not completely read file " + file.getName());
    }

    is.close();
    return bytes;
  }

  private void closeCache(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        closeCache();
        return null;
      }
    });
  }


  private int createServerRegion(VM vm) {
    var createRegion = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        var af = new AttributesFactory();
        // af.setScope(Scope.DISTRIBUTED_ACK);
        af.setDataPolicy(DataPolicy.PARTITION);
        createRootRegion("testSimplePdx", af.create());

        var server = getCache().addCacheServer();
        var port = AvailablePortHelper.getRandomAvailableTCPPort();
        server.setPort(port);
        server.start();
        return port;
      }
    };

    return (Integer) vm.invoke(createRegion);
  }

  private int createServerRegion(VM vm, final boolean isPdxReadSerialized) {
    var createRegion = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        var af = new AttributesFactory();
        // af.setScope(Scope.DISTRIBUTED_ACK);
        af.setDataPolicy(DataPolicy.PARTITION);
        createRootRegion("testSimplePdx", af.create());

        getCache().getCacheConfig().setPdxReadSerialized(isPdxReadSerialized);

        var server = getCache().addCacheServer();
        var port = AvailablePortHelper.getRandomAvailableTCPPort();
        server.setPort(port);
        server.start();
        return port;
      }
    };

    return (Integer) vm.invoke(createRegion);
  }

  private int createServerRegionWithPersistence(VM vm, final boolean persistentPdxRegistry) {
    var createRegion = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        var cf = new CacheFactory();
        if (persistentPdxRegistry) {
          cf.setPdxPersistent(true).setPdxDiskStore("store");
        }
        //
        Cache cache = getCache(cf);
        cache.createDiskStoreFactory().setDiskDirs(getDiskDirs()).create("store");

        var af = new AttributesFactory();
        af.setScope(Scope.DISTRIBUTED_ACK);
        af.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
        af.setDiskStoreName("store");
        createRootRegion("testSimplePdx", af.create());

        var server = getCache().addCacheServer();
        var port = AvailablePortHelper.getRandomAvailableTCPPort();
        server.setPort(port);
        server.start();
        return port;
      }
    };

    return (Integer) vm.invoke(createRegion);
  }

  private int createServerAccessor(VM vm) {
    var createRegion = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        var af = new AttributesFactory();
        // af.setScope(Scope.DISTRIBUTED_ACK);
        af.setDataPolicy(DataPolicy.EMPTY);
        createRootRegion("testSimplePdx", af.create());

        var server = getCache().addCacheServer();
        var port = AvailablePortHelper.getRandomAvailableTCPPort();
        server.setPort(port);
        server.start();
        return port;
      }
    };

    return (Integer) vm.invoke(createRegion);
  }

  private int createLonerServerRegion(VM vm, final String regionName, final String dsId) {
    var createRegion = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        var props = new Properties();
        props.setProperty(LOCATORS, "");
        props.setProperty(DISTRIBUTED_SYSTEM_ID, dsId);
        getSystem(props);
        var af = new AttributesFactory();
        af.setScope(Scope.DISTRIBUTED_ACK);
        af.setDataPolicy(DataPolicy.REPLICATE);
        createRootRegion(regionName, af.create());

        var server = getCache().addCacheServer();
        var port = AvailablePortHelper.getRandomAvailableTCPPort();
        server.setPort(port);
        server.start();
        return port;
      }
    };

    return (Integer) vm.invoke(createRegion);
  }

  private void createClientRegion(final VM vm, final int port) {
    var createRegion = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        var cf = new ClientCacheFactory();
        cf.addPoolServer(NetworkUtils.getServerHostName(vm.getHost()), port);
        var cache = getClientCache(cf);
        cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create("testSimplePdx");
        return null;
      }
    };
    vm.invoke(createRegion);
  }

  private void createClientRegion(final VM vm, final int port, final boolean isPdxReadSerialized) {
    var createRegion = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        var cf = new ClientCacheFactory();
        cf.addPoolServer(NetworkUtils.getServerHostName(vm.getHost()), port);
        cf.setPdxReadSerialized(isPdxReadSerialized);
        var cache = getClientCache(cf);
        cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create("testSimplePdx");
        return null;
      }
    };
    vm.invoke(createRegion);
  }


}
