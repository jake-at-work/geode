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
 *
 */

package org.apache.geode.redis.internal.data;

import static org.apache.geode.internal.JvmSizeUtils.memoryOverhead;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_INTEGER;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_OVERFLOW;
import static org.apache.geode.redis.internal.RedisConstants.HASH_VALUE_NOT_FLOAT;
import static org.apache.geode.redis.internal.RedisConstants.REDIS_HASH_DATA_SERIALIZABLE_ID;
import static org.apache.geode.redis.internal.netty.Coder.bytesToLong;
import static org.apache.geode.redis.internal.netty.Coder.bytesToString;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.lang3.tuple.ImmutablePair;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.Instantiator;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.Region;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.redis.internal.commands.executor.GlobPattern;
import org.apache.geode.redis.internal.data.collections.SizeableBytes2ObjectOpenCustomHashMapWithCursor;
import org.apache.geode.redis.internal.data.delta.AddByteArrayPairs;
import org.apache.geode.redis.internal.data.delta.RemoveByteArrays;
import org.apache.geode.redis.internal.netty.Coder;

public class RedisHash extends AbstractRedisData {

  static {
    Instantiator.register(new Instantiator(RedisHash.class, REDIS_HASH_DATA_SERIALIZABLE_ID) {
      public DataSerializable newInstance() {
        return new RedisHash();
      }
    });
  }

  protected static final int REDIS_HASH_OVERHEAD = memoryOverhead(RedisHash.class);

  private Hash hash;

  @VisibleForTesting
  public RedisHash(List<byte[]> fieldsToSet) {
    final var numKeysAndValues = fieldsToSet.size();
    if (numKeysAndValues % 2 != 0) {
      throw new IllegalStateException(
          "fieldsToSet should have an even number of elements but was size " + numKeysAndValues);
    }

    hash = new Hash(numKeysAndValues / 2);
    var iterator = fieldsToSet.iterator();
    while (iterator.hasNext()) {
      hashPut(iterator.next(), iterator.next());
    }
  }

  /**
   * For deserialization only.
   */
  public RedisHash() {}

  /**
   * Since GII (getInitialImage) can come in and call toData while other threads are modifying this
   * object, the striped executor will not protect toData. So any methods that modify "hash" needs
   * to be thread safe with toData.
   */
  @Override
  public synchronized void toData(DataOutput out) throws IOException {
    super.toData(out);
    hash.toData(out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    int size = DataSerializer.readInteger(in);
    hash = new Hash(size);
    for (var i = 0; i < size; i++) {
      hash.put(DataSerializer.readByteArray(in), DataSerializer.readByteArray(in));
    }
  }

  synchronized byte[] hashPut(byte[] field, byte[] value) {
    return hash.put(field, value);
  }

  private synchronized byte[] hashPutIfAbsent(byte[] field, byte[] value) {
    return hash.putIfAbsent(field, value);
  }

  private synchronized byte[] hashRemove(byte[] field) {
    return hash.remove(field);
  }

  @Override
  public void applyAddByteArrayPairDelta(byte[] keyBytes, byte[] valueBytes) {
    hashPut(keyBytes, valueBytes);
  }

  @Override
  public void applyRemoveByteArrayDelta(byte[] bytes) {
    hashRemove(bytes);
  }

  public int hset(Region<RedisKey, RedisData> region, RedisKey key,
      List<byte[]> fieldsToSet, boolean nx) {
    var fieldsAdded = 0;
    AddByteArrayPairs deltaInfo = null;
    var iterator = fieldsToSet.iterator();
    while (iterator.hasNext()) {
      var field = iterator.next();
      var value = iterator.next();
      boolean newField;
      boolean addedOrUpdated;
      if (nx) {
        newField = hashPutIfAbsent(field, value) == null;
        addedOrUpdated = newField;
      } else {
        newField = hashPut(field, value) == null;
        addedOrUpdated = true;
      }

      if (addedOrUpdated) {
        if (deltaInfo == null) {
          deltaInfo = new AddByteArrayPairs(fieldsToSet.size());
        }
        deltaInfo.add(field, value);
      }

      if (newField) {
        fieldsAdded++;
      }
    }
    storeChanges(region, key, deltaInfo);

    return fieldsAdded;
  }

  public int hdel(Region<RedisKey, RedisData> region, RedisKey key, List<byte[]> fieldsToRemove) {
    var fieldsRemoved = 0;
    RemoveByteArrays deltaInfo = null;
    for (var fieldToRemove : fieldsToRemove) {
      if (hashRemove(fieldToRemove) != null) {
        if (deltaInfo == null) {
          deltaInfo = new RemoveByteArrays();
        }
        deltaInfo.add(fieldToRemove);
        fieldsRemoved++;
      }
    }
    storeChanges(region, key, deltaInfo);
    return fieldsRemoved;
  }

  public Collection<byte[]> hgetall() {
    var result = new ArrayList<byte[]>(hash.size() * 2);
    hash.fastForEach(entry -> {
      result.add(entry.getKey());
      result.add(entry.getValue());
    });
    return result;
  }

  public int hexists(byte[] field) {
    if (hash.containsKey(field)) {
      return 1;
    } else {
      return 0;
    }
  }

  public byte[] hget(byte[] field) {
    return hash.get(field);
  }

  public int hlen() {
    return hash.size();
  }

  public int hstrlen(byte[] field) {
    var entry = hget(field);
    return entry != null ? entry.length : 0;
  }

  public List<byte[]> hmget(List<byte[]> fields) {
    var results = new ArrayList<byte[]>(fields.size());
    for (var field : fields) {
      results.add(hash.get(field));
    }
    return results;
  }

  public Collection<byte[]> hvals() {
    var result = new ArrayList<byte[]>(hlen());
    hash.fastForEachValue(result::add);
    return result;
  }

  public Collection<byte[]> hkeys() {
    var result = new ArrayList<byte[]>(hlen());
    hash.fastForEachKey(result::add);
    return result;
  }

  public ImmutablePair<Integer, List<byte[]>> hscan(GlobPattern matchPattern, int count,
      int cursor) {
    // No need to allocate more space than it's possible to use given the size of the hash. We need
    // to add 1 to hlen() to ensure that if count > hash.size(), we return a cursor of 0
    var maximumCapacity = 2L * Math.min(count, hlen() + 1);
    if (maximumCapacity > Integer.MAX_VALUE) {
      LogService.getLogger().info(
          "The size of the data to be returned by hscan, {}, exceeds the maximum capacity of an array. A value for the HSCAN COUNT argument less than {} should be used",
          maximumCapacity, Integer.MAX_VALUE / 2);
      throw new IllegalArgumentException("Requested array size exceeds VM limit");
    }
    List<byte[]> resultList = new ArrayList<>((int) maximumCapacity);

    cursor = hash.scan(cursor, count,
        (list, key, value) -> addIfMatching(matchPattern, list, key, value), resultList);

    return new ImmutablePair<>(cursor, resultList);
  }

  private void addIfMatching(GlobPattern matchPattern, List<byte[]> resultList, byte[] key,
      byte[] value) {
    if (matchPattern != null) {
      if (matchPattern.matches(key)) {
        resultList.add(key);
        resultList.add(value);
      }
    } else {
      resultList.add(key);
      resultList.add(value);
    }
  }

  public byte[] hincrby(Region<RedisKey, RedisData> region, RedisKey key, byte[] field,
      long increment) throws NumberFormatException, ArithmeticException {
    var oldValue = hash.get(field);
    if (oldValue == null) {
      var newValue = Coder.longToBytes(increment);
      hashPut(field, newValue);
      storeChanges(region, key, new AddByteArrayPairs(field, newValue));
      return newValue;
    }

    long value;
    try {
      value = bytesToLong(oldValue);
    } catch (NumberFormatException ex) {
      throw new NumberFormatException(ERROR_NOT_INTEGER);
    }
    if ((value >= 0 && increment > (Long.MAX_VALUE - value))
        || (value <= 0 && increment < (Long.MIN_VALUE - value))) {
      throw new ArithmeticException(ERROR_OVERFLOW);
    }

    value += increment;

    var modifiedValue = Coder.longToBytes(value);
    hashPut(field, modifiedValue);
    storeChanges(region, key, new AddByteArrayPairs(field, modifiedValue));
    return modifiedValue;
  }

  public BigDecimal hincrbyfloat(Region<RedisKey, RedisData> region, RedisKey key,
      byte[] field, BigDecimal increment) throws NumberFormatException {
    var oldValue = hash.get(field);
    if (oldValue == null) {
      var newValue = Coder.bigDecimalToBytes(increment);
      hashPut(field, newValue);
      storeChanges(region, key, new AddByteArrayPairs(field, newValue));
      return increment.stripTrailingZeros();
    }

    var valueS = bytesToString(oldValue);
    if (valueS.contains(" ")) {
      throw new NumberFormatException(HASH_VALUE_NOT_FLOAT);
    }

    BigDecimal value;
    try {
      value = new BigDecimal(valueS);
    } catch (NumberFormatException ex) {
      throw new NumberFormatException(HASH_VALUE_NOT_FLOAT);
    }

    value = value.add(increment);

    var modifiedValue = Coder.bigDecimalToBytes(value);
    hashPut(field, modifiedValue);
    storeChanges(region, key, new AddByteArrayPairs(field, modifiedValue));
    return value.stripTrailingZeros();
  }

  @Override
  public RedisDataType getType() {
    return RedisDataType.REDIS_HASH;
  }

  @Override
  protected boolean removeFromRegion() {
    return hash.isEmpty();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RedisHash)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    var redisHash = (RedisHash) o;
    if (hash.size() != redisHash.hash.size()) {
      return false;
    }

    return hash.fastWhileEach(
        entry -> Arrays.equals(redisHash.hash.get(entry.getKey()), entry.getValue()));
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), hash);
  }

  @Override
  public String toString() {
    return "RedisHash{" + super.toString() + ", " + "size=" + hash.size() + "}";
  }

  @Override
  public int getSizeInBytes() {
    return REDIS_HASH_OVERHEAD + hash.getSizeInBytes();
  }

  public static class Hash
      extends SizeableBytes2ObjectOpenCustomHashMapWithCursor<byte[]> {

    public Hash() {
      super();
    }

    public Hash(int expected) {
      super(expected);
    }

    public Hash(Map<byte[], byte[]> m) {
      super(m);
    }

    @Override
    protected int sizeValue(byte[] value) {
      return sizeKey(value);
    }

    public void toData(DataOutput out) throws IOException {
      DataSerializer.writePrimitiveInt(size(), out);
      final var maxIndex = getMaxIndex();
      for (var pos = 0; pos < maxIndex; ++pos) {
        var key = getKeyAtIndex(pos);
        if (key != null) {
          DataSerializer.writeByteArray(key, out);
          DataSerializer.writeByteArray(getValueAtIndex(pos), out);
        }
      }
    }
  }
}
