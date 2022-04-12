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
package org.apache.geode.internal.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import org.apache.bcel.Constants;
import org.apache.bcel.generic.ClassGen;
import org.apache.bcel.generic.FieldGen;
import org.apache.bcel.generic.InstructionFactory;
import org.apache.bcel.generic.InstructionList;
import org.apache.bcel.generic.MethodGen;
import org.apache.bcel.generic.Type;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


/**
 * Unit tests for {@link BlobHelper} with Thread Context ClassLoader.
 *
 * @since GemFire 2.0.2
 */
public class BlobHelperWithThreadContextClassLoaderTest {

  private static final String CLASS_NAME_SERIALIZABLE_IMPL =
      "org.apache.geode.internal.util.SerializableImpl";
  private static final String CLASS_NAME_SERIALIZABLE_IMPL_WITH_VALUE =
      "org.apache.geode.internal.util.SerializableImplWithValue";
  private static final String VALUE = "value";
  private static final String SET_VALUE = "setValue";
  private static final String GET_VALUE = "getValue";

  private ClassLoader oldCCL;

  @Before
  public void setUp() throws MalformedURLException {
    oldCCL = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(new GeneratingClassLoader(oldCCL));
  }

  @After
  public void tearDown() {
    Thread.currentThread().setContextClassLoader(oldCCL);
  }

  @Test
  public void tcclLoadsSerializableImpl() throws Exception {
    Class loadedClass = Class.forName(CLASS_NAME_SERIALIZABLE_IMPL, true,
        Thread.currentThread().getContextClassLoader());
    assertThat(loadedClass).isNotNull();
    assertThat(loadedClass.getName()).isEqualTo(CLASS_NAME_SERIALIZABLE_IMPL);

    var instance = loadedClass.newInstance();
    assertThat(instance).isNotNull();
    assertThat(loadedClass instanceof Serializable);
    assertThat(loadedClass.getInterfaces()).contains(Serializable.class);
  }

  @Test
  public void tcclLoadsSerializableImplWithValue() throws Exception {
    Class loadedClass = Class.forName(CLASS_NAME_SERIALIZABLE_IMPL_WITH_VALUE, true,
        Thread.currentThread().getContextClassLoader());
    assertThat(loadedClass).isNotNull();
    assertThat(loadedClass.getName()).isEqualTo(CLASS_NAME_SERIALIZABLE_IMPL_WITH_VALUE);

    var instance = loadedClass.newInstance();
    assertThat(instance).isNotNull();

    assertThat(loadedClass.getSuperclass().getName()).isEqualTo(CLASS_NAME_SERIALIZABLE_IMPL);
    assertThat(loadedClass instanceof Serializable);

    assertThat(Valuable.class.isInstance(loadedClass));
    assertThat(loadedClass.getInterfaces()).contains(Valuable.class);

    var setter = loadedClass.getMethod("setValue", Object.class);
    assertThat(setter).isNotNull();
  }

  /**
   * Tests serializing an object loaded with the current context class loader (whose parent is the
   * loader that loads GemFire and test classes).
   */
  @Test
  public void handlesClassFromOtherClassLoader() throws Exception {
    Class loadedClass = Class.forName(CLASS_NAME_SERIALIZABLE_IMPL, true,
        Thread.currentThread().getContextClassLoader());

    var instance = loadedClass.newInstance();
    var bytes = BlobHelper.serializeToBlob(instance);

    var object = BlobHelper.deserializeBlob(bytes);

    assertThat(object).isNotNull();
    assertThat(object.getClass().getName()).isEqualTo(CLASS_NAME_SERIALIZABLE_IMPL);
    assertThat(object instanceof Serializable);

    Class deserializedClass = object.getClass();
    assertThat(deserializedClass.getInterfaces()).contains(Serializable.class);
  }

  /**
   * Tests that the deserialized object has the correct state
   */
  @Test
  public void handlesObjectWithStateFromOtherClassLoader() throws Exception {
    Class loadedClass = Class.forName(CLASS_NAME_SERIALIZABLE_IMPL_WITH_VALUE, true,
        Thread.currentThread().getContextClassLoader());

    var ctor = loadedClass.getConstructor(Object.class);
    var instance = (Valuable) ctor.newInstance(new Object[] {123});
    assertThat(instance.getValue()).isEqualTo(123);

    var bytes = BlobHelper.serializeToBlob(instance);

    var object = (Valuable) BlobHelper.deserializeBlob(bytes);
    assertThat(object.getValue()).isEqualTo(instance.getValue());
  }

  /**
   * Custom class loader which uses BCEL to dynamically generate SerializableImpl or
   * SerializableImplWithValue.
   */
  private static class GeneratingClassLoader extends ClassLoader {

    private static final String GENERATED = "<generated>";

    private final Map<String, Class<?>> classDefinitions;

    public GeneratingClassLoader(ClassLoader parent) {
      super(parent);
      classDefinitions = new HashMap<>();
    }

    public GeneratingClassLoader() {
      this(null); // no parent
    }

    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
      Class<?> definedClass = null;
      synchronized (classDefinitions) {
        definedClass = getClass(name);
        if (definedClass == null) {
          definedClass = generate(name);
          classDefinitions.put(name, definedClass);
        }
      }
      return definedClass;
    }

    @Override
    protected URL findResource(String name) {
      return null;
    }

    @Override
    protected Enumeration<URL> findResources(String name) throws IOException {
      return null;
    }

    private Class<?> generate(String name) throws ClassNotFoundException {
      if (CLASS_NAME_SERIALIZABLE_IMPL.equals(name)) {
        return generateSerializableImpl();
      } else if (CLASS_NAME_SERIALIZABLE_IMPL_WITH_VALUE.equals(name)) {
        return generateSerializableImplWithValue();
      } else {
        return null;
        // throw new Error("Unable to generate " + name);
      }
    }

    /**
     * <pre>
     * public class SerializableImpl implements Serializable {
     *
     *   public SerializableImpl() {}
     *
     * }
     * </pre>
     */
    private Class<?> generateSerializableImpl() throws ClassNotFoundException {
      var cg = new ClassGen(CLASS_NAME_SERIALIZABLE_IMPL, Object.class.getName(), GENERATED,
          Constants.ACC_PUBLIC | Constants.ACC_SUPER, new String[] {Serializable.class.getName()});
      cg.addEmptyConstructor(Constants.ACC_PUBLIC);
      var jClazz = cg.getJavaClass();
      var bytes = jClazz.getBytes();
      return defineClass(jClazz.getClassName(), bytes, 0, bytes.length);
    }

    /**
     * <pre>
     * public class SerializableImplWithValue extends SerializableImpl implements Valuable {
     *
     *   private Object value;
     *
     *   public SerializableImplWithValue() {}
     *
     *   public SerializableImplWithValue(Object value) {
     *     this.value = value;
     *   }
     *
     *   public Object getValue() {
     *     return this.value;
     *   }
     *
     *   public void setValue(Object value) {
     *     this.value = value;
     *   }
     * }
     * </pre>
     *
     * @see Valuable
     */
    private Class<?> generateSerializableImplWithValue() throws ClassNotFoundException {
      var cg = new ClassGen(CLASS_NAME_SERIALIZABLE_IMPL_WITH_VALUE,
          CLASS_NAME_SERIALIZABLE_IMPL, GENERATED, Constants.ACC_PUBLIC | Constants.ACC_SUPER,
          new String[] {Valuable.class.getName()});
      var cp = cg.getConstantPool();
      var fac = new InstructionFactory(cg, cp);

      // field
      var fg = new FieldGen(Constants.ACC_PRIVATE, Type.OBJECT, VALUE, cp);
      var field = fg.getField();
      cg.addField(field);

      // empty constructor
      cg.addEmptyConstructor(Constants.ACC_PUBLIC);

      // constructor with arg
      var ctor = new InstructionList();
      var ctorMethod = new MethodGen(Constants.ACC_PUBLIC, Type.VOID,
          new Type[] {Type.OBJECT}, new String[] {"arg0"}, "<init>",
          "org.apache.geode.internal.util.bcel.SerializableImplWithValue", ctor, cp);
      ctorMethod.setMaxStack(2);

      var ctor_ih_0 = ctor.append(InstructionFactory.createLoad(Type.OBJECT, 0));
      ctor.append(fac.createInvoke(CLASS_NAME_SERIALIZABLE_IMPL, "<init>", Type.VOID, Type.NO_ARGS,
          Constants.INVOKESPECIAL));
      var ctor_ih_4 = ctor.append(InstructionFactory.createLoad(Type.OBJECT, 0));
      ctor.append(InstructionFactory.createLoad(Type.OBJECT, 1));
      ctor.append(fac.createFieldAccess(CLASS_NAME_SERIALIZABLE_IMPL_WITH_VALUE, "value",
          Type.OBJECT, Constants.PUTFIELD));
      var ctor_ih_9 = ctor.append(InstructionFactory.createReturn(Type.VOID));

      cg.addMethod(ctorMethod.getMethod());
      ctor.dispose();

      // getter
      var getter = new InstructionList();
      var getterMethod = new MethodGen(Constants.ACC_PUBLIC, Type.OBJECT, null, null,
          GET_VALUE, CLASS_NAME_SERIALIZABLE_IMPL_WITH_VALUE, getter, cp);
      getterMethod.setMaxStack(1);

      var getter_ih_0 = getter.append(InstructionFactory.createLoad(Type.OBJECT, 0));
      var getter_ih_1 = getter.append(fac.createGetField(cg.getClassName(),
          field.getName(), Type.getType(field.getSignature())));
      var getter_ih_4 = getter.append(InstructionFactory.createReturn(Type.OBJECT));

      cg.addMethod(getterMethod.getMethod());
      getter.dispose();

      // setter
      var setter = new InstructionList();
      var setterMethod = new MethodGen(Constants.ACC_PUBLIC, Type.VOID,
          new Type[] {Type.OBJECT}, new String[] {field.getName()}, SET_VALUE,
          CLASS_NAME_SERIALIZABLE_IMPL_WITH_VALUE, setter, cp);
      setterMethod.setMaxStack(2);

      var setter_ih_0 = setter.append(InstructionFactory.createLoad(Type.OBJECT, 0));
      var setter_ih_1 = setter.append(InstructionFactory.createLoad(Type.OBJECT, 1));
      var setter_ih_2 = setter.append(fac.createPutField(cg.getClassName(),
          field.getName(), Type.getType(field.getSignature())));
      var setter_ih_0_ih_5 =
          setter.append(InstructionFactory.createReturn(Type.VOID));

      cg.addMethod(setterMethod.getMethod());
      setter.dispose();

      var jClazz = cg.getJavaClass();
      var bytes = jClazz.getBytes();
      return defineClass(jClazz.getClassName(), bytes, 0, bytes.length);
    }

    private Class<?> getClass(String name) {
      synchronized (classDefinitions) {
        return classDefinitions.get(name);
      }
    }
  }
}
