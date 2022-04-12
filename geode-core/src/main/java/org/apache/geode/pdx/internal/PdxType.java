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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.StaticSerialization;
import org.apache.geode.pdx.PdxFieldAlreadyExistsException;
import org.apache.geode.pdx.internal.AutoSerializableManager.AutoClassInfo;

public class PdxType implements DataSerializable {

  private static final long serialVersionUID = -1950047949756115279L;

  private int cachedHash = 0;

  private int typeId;
  private String className;
  private boolean noDomainClass;
  /**
   * Will be set to true if any fields on this type have been deleted.
   *
   * @since GemFire 8.1
   */
  private boolean hasDeletedField;

  /**
   * A count of the total number of variable length field offsets.
   */
  private int vlfCount;

  private final ArrayList<PdxField> fields = new ArrayList<>();

  private final transient Map<String, PdxField> fieldsMap = new HashMap<>();
  private transient volatile SortedSet<PdxField> sortedIdentityFields;

  public PdxType() {
    // for deserialization
  }

  public PdxType(String name, boolean expectDomainClass) {
    className = name;
    noDomainClass = !expectDomainClass;
    swizzleGemFireClassNames();
  }

  public PdxType(PdxType copy) {
    typeId = copy.typeId;
    className = copy.className;
    noDomainClass = copy.noDomainClass;
    vlfCount = copy.vlfCount;
    for (var ft : copy.fields) {
      addField(ft);
    }
  }

  private static final byte NO_DOMAIN_CLASS_BIT = 1;
  private static final byte HAS_DELETED_FIELD_BIT = 2;

  private void swizzleGemFireClassNames() {
    var svc = InternalDataSerializer.getOldClientSupportService();
    if (svc != null) {
      className = svc.processIncomingClassName(className);
    }
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    className = DataSerializer.readString(in);
    swizzleGemFireClassNames();
    {
      var bits = in.readByte();
      noDomainClass = (bits & NO_DOMAIN_CLASS_BIT) != 0;
      hasDeletedField = (bits & HAS_DELETED_FIELD_BIT) != 0;
    }

    typeId = in.readInt();
    vlfCount = in.readInt();

    var arrayLen = InternalDataSerializer.readArrayLength(in);

    for (var i = 0; i < arrayLen; i++) {
      var vft = new PdxField();
      vft.fromData(in);
      addField(vft);
    }
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(className, out);
    {
      // pre 8.1 we wrote a single boolean
      // 8.1 and after we write a byte whose bits are:
      // 1: noDomainClass
      // 2: hasDeletedField
      byte bits = 0;
      if (noDomainClass) {
        bits |= NO_DOMAIN_CLASS_BIT;
      }
      // Note that this code attempts to only set the HAS_DELETED_FIELD_BIT
      // if serializing for 8.1 or later.
      // But in some cases 8.1 serialized data may be sent to a pre 8.1 member.
      // In that case if this bit is set it will cause the pre 8.1 member
      // to set noDomainClass to true.
      // For this reason the pdx delete-field command should only be used after
      // all member have been upgraded to 8.1 or later.
      var sourceVersion = StaticSerialization.getVersionForDataStream(out);
      if (sourceVersion.isNotOlderThan(KnownVersion.GFE_81)) {
        if (hasDeletedField) {
          bits |= HAS_DELETED_FIELD_BIT;
        }
      }
      out.writeByte(bits);
    }

    out.writeInt(typeId);
    out.writeInt(vlfCount);

    InternalDataSerializer.writeArrayLength(fields.size(), out);

    for (var vft : fields) {

      vft.toData(out);
    }
  }

  @Override
  public int hashCode() {
    var hash = cachedHash;
    if (hash == 0) {
      hash = 1;
      hash = hash * 31 + className.hashCode();
      for (var field : fields) {
        hash = hash * 31 + field.hashCode();
      }
      if (hash == 0) {
        hash = 1;
      }
      cachedHash = hash;
    }
    return cachedHash;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if (!(other instanceof PdxType)) {
      return false;
    }
    // Note: do not compare type id in equals
    var otherVT = (PdxType) other;
    if (!(className.equals(otherVT.className))) {
      return false;
    }
    if (noDomainClass != otherVT.noDomainClass) {
      return false;
    }
    if (otherVT.fields.size() != fields.size() || otherVT.vlfCount != vlfCount) {
      return false;
    }
    for (var i = 0; i < fields.size(); i++) {
      if (!fields.get(i).equals(otherVT.fields.get(i))) {
        return false;
      }
    }
    return true;
  }

  /**
   * Return true if two pdx types have same class name and the same fields but, unlike equals, field
   * order does not matter. Note a type that expects a domain class can be compatible with one that
   * does not expect a domain class.
   *
   * @param other the other pdx type
   * @return true if two pdx types are compatible.
   */
  public boolean compatible(PdxType other) {
    if (other == null) {
      return false;
    }
    if (!getClassName().equals(other.getClassName())) {
      return false;
    }

    var myFields = getSortedFields();
    var otherFields = other.getSortedFields();

    return myFields.equals(otherFields);
  }

  public int getVariableLengthFieldCount() {
    return vlfCount;
  }

  public String getClassName() {
    return className;
  }

  public boolean getNoDomainClass() {
    return noDomainClass;
  }

  public void setNoDomainClass(boolean noDomainClass) {
    this.noDomainClass = noDomainClass;
  }

  public int getTypeId() {
    return typeId;
  }

  public int getDSId() {
    return typeId >> 24 & 0xFF;
  }

  public int getTypeNum() {
    return typeId & 0x00FFFFFF;
  }

  public void setTypeId(int tId) {
    typeId = tId;
  }

  /*
   * This method is use to create Pdxtype for which classname is not available; while creating
   * PdxInstance
   */
  public void setClassName(String className) {
    this.className = className;
  }

  public void addField(PdxField ft) {
    if (fieldsMap.put(ft.getFieldName(), ft) != null) {
      throw new PdxFieldAlreadyExistsException(
          "The field \"" + ft.getFieldName() + "\" already exists.");
    }
    fields.add(ft);
  }

  public void initialize(PdxWriterImpl writer) {
    vlfCount = writer.getVlfCount();
    var size = fields.size();
    var fixedLenFieldOffset = 0;
    var seenVariableLenType = false;
    for (var i = 0; i < size; i++) {
      var vft = fields.get(i);
      // System.out.println(i + ": " + vft);
      if (vft.isVariableLengthType()) {
        if (seenVariableLenType) {
          vft.setVlfOffsetIndex(vft.getVarLenFieldSeqId());
        } else {
          vft.setRelativeOffset(fixedLenFieldOffset);
          vft.setVlfOffsetIndex(-1);
        }
        seenVariableLenType = true;
      } else if (seenVariableLenType) {
        PdxField tmp = null;
        var minusOffset = vft.getFieldType().getWidth();
        for (var j = (i + 1); j < size; j++) {
          tmp = fields.get(j);
          if (tmp.isVariableLengthType()) {
            break;
          } else {
            minusOffset += tmp.getFieldType().getWidth();
          }
        }
        if (tmp != null && tmp.isVariableLengthType()) {
          vft.setRelativeOffset(-minusOffset);
          vft.setVlfOffsetIndex(tmp.getVarLenFieldSeqId());
        } else {
          vft.setRelativeOffset(-minusOffset);
          vft.setVlfOffsetIndex(-1); // From the byte after the last field data byte
        }
      } else {
        vft.setRelativeOffset(fixedLenFieldOffset);
        fixedLenFieldOffset += vft.getFieldType().getWidth();
      }
    }
    // no longer mark identity fields implicitly. Fixes bug 42976.

    // System.out.println("Printing the position array:");
    // for (int i = 0; i < this.positionArray.length; i++) {
    // System.out.println("[" + i + "][0]=" + this.positionArray[i][0] + ", ["
    // + i + "][1]=" + this.positionArray[i][1]);
    // }
  }

  public PdxField getPdxField(String fieldName) {
    var result = fieldsMap.get(fieldName);
    if (result != null && result.isDeleted()) {
      result = null;
    }
    return result;
  }

  public List<PdxField> getFields() {
    return Collections.unmodifiableList(fields);
  }

  public PdxField getPdxFieldByIndex(int index) {
    return fields.get(index);
  }

  public int getFieldCount() {
    return fields.size();
  }

  public int getUndeletedFieldCount() {
    if (!getHasDeletedField()) {
      return 0;
    }
    var result = fields.size();
    for (var f : fields) {
      if (f.isDeleted()) {
        result--;
      }
    }
    return result;
  }

  public String toFormattedString() {
    var sb = new StringBuilder("PdxType[");
    sb.append("dsid=").append(getDSId());
    sb.append(", typenum=").append(getTypeNum());
    sb.append("\n        name=").append(className);
    sb.append("\n        fields=[");
    for (var vft : fields) {
      sb.append("\n        ");
      sb.append(/* vft.getFieldName() + ":" + vft.getTypeId() */ vft.toString());
    }
    sb.append("]]");
    return sb.toString();
  }

  public String toString() {
    var sb = new StringBuilder("PdxType[");
    sb.append("dsid=").append(getDSId());
    sb.append(",typenum=").append(getTypeNum());
    sb.append(",name=").append(className);
    sb.append(",fields=[");
    for (var vft : fields) {
      sb.append(/* vft.getFieldName() + ":" + vft.getTypeId() */ vft.toString()).append(", ");
    }
    sb.append("]]");
    return sb.toString();
  }

  /**
   *
   * @param readFields the fields that have been read
   * @return a List of fields that have not been read (may be empty).
   */
  public List<Integer> getUnreadFieldIndexes(List<String> readFields) {
    var result = new ArrayList<Integer>();
    for (var ft : fields) {
      if (!ft.isDeleted() && !readFields.contains(ft.getFieldName())) {
        result.add(ft.getFieldIndex());
      }
    }
    return result;
  }

  /**
   * Return true if the this type has a field that the other type does not have.
   *
   * @param other the type we are comparing to
   * @return true if the this type has a field that the other type does not have.
   */
  public boolean hasExtraFields(PdxType other) {
    for (var ft : fields) {
      if (!ft.isDeleted() && other.getPdxField(ft.getFieldName()) == null) {
        return true;
      }
    }
    return false;
  }

  // Result does not include deleted fields
  public SortedSet<PdxField> getSortedIdentityFields() {
    if (sortedIdentityFields == null) {
      var sortedSet = new TreeSet<PdxField>();
      for (var field : fields) {
        if (field.isIdentityField() && !field.isDeleted()) {
          sortedSet.add(field);
        }
      }
      // If we don't find any marked identity fields, use all of the fields.
      if (sortedSet.isEmpty()) {
        for (var field : fields) {
          if (!field.isDeleted()) {
            sortedSet.add(field);
          }
        }
      }
      sortedIdentityFields = sortedSet;
    }
    return sortedIdentityFields;
  }

  // Result does not include deleted fields
  public Collection<PdxField> getSortedFields() {
    var sortedSet = new TreeSet<PdxField>();
    for (var pf : fields) {
      if (!pf.isDeleted()) {
        sortedSet.add(pf);
      }
    }
    return new ArrayList<>(sortedSet);
  }

  // Result does not include deleted fields
  public List<String> getFieldNames() {
    var result = new ArrayList<String>(fields.size());
    for (var f : fields) {
      if (!f.isDeleted()) {
        result.add(f.getFieldName());
      }
    }
    return Collections.unmodifiableList(result);
  }

  /**
   * Used to optimize auto deserialization
   */
  private final transient AtomicReference<AutoClassInfo> autoClassInfo =
      new AtomicReference<>();

  public void setAutoInfo(AutoClassInfo autoClassInfo) {
    this.autoClassInfo.set(autoClassInfo);
  }

  public AutoClassInfo getAutoInfo(Class<?> c) {
    var ci = autoClassInfo.get();
    if (ci != null) {
      var lastClassAutoSerialized = ci.getInfoClass();
      if (c.equals(lastClassAutoSerialized)) {
        return ci;
      } else {
        if (lastClassAutoSerialized == null) {
          autoClassInfo.compareAndSet(ci, null);
        }
      }
    }
    return null;
  }

  public void toStream(PrintStream printStream, boolean printFields) {
    printStream.print("  ");
    printStream.print(getClassName());
    printStream.print(": ");
    printStream.print("id=");
    printStream.print(getTypeNum());
    if (getDSId() != 0) {
      printStream.print(" dsId=");
      printStream.print(getDSId());
    }
    printStream.println();
    if (printFields) {
      for (var field : fields) {
        field.toStream(printStream);
      }
    }
  }

  public boolean getHasDeletedField() {
    return hasDeletedField;
  }

  public void setHasDeletedField(boolean b) {
    hasDeletedField = b;
  }
}
