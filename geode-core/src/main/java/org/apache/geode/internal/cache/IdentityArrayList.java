/*
 * Copyright (C) 2002-2014 Sebastiano Vigna
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

/*
 * This is a modified version of the fastutil ObjectArrayList. It is modified to use identity checks
 * rather than equality checks for higher performance.
 */
package org.apache.geode.internal.cache;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.RandomAccess;

import it.unimi.dsi.fastutil.objects.AbstractObjectList;
import it.unimi.dsi.fastutil.objects.AbstractObjectListIterator;
import it.unimi.dsi.fastutil.objects.ObjectArrays;
import it.unimi.dsi.fastutil.objects.ObjectCollection;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import it.unimi.dsi.fastutil.objects.ObjectIterators;
import it.unimi.dsi.fastutil.objects.ObjectList;
import it.unimi.dsi.fastutil.objects.ObjectListIterator;

/**
 *
 *
 * A type-specific array-based list; provides some additional methods that use polymorphism to avoid
 * (un)boxing.
 *
 * <P>
 * This class implements a lightweight, fast, open, optimized, reuse-oriented version of array-based
 * lists. Instances of this class represent a list with an array that is enlarged as needed when new
 * entries are created (by doubling the current length), but is <em>never</em> made smaller (even on
 * a {@link #clear()}). A family of {@linkplain #trim() trimming methods} lets you control the size
 * of the backing array; this is particularly useful if you reuse instances of this class. Range
 * checks are equivalent to those of <code>java.util</code>'s classes, but they are delayed as much
 * as possible.
 *
 * <p>
 * The backing array is exposed by the {@link #elements()} method. If an instance of this class was
 * created {@linkplain #wrap(Object[],int) by wrapping}, backing-array reallocations will be
 * performed using reflection, so that {@link #elements()} can return an array of the same type of
 * the original array; the comments about efficiency made in
 * {@link it.unimi.dsi.fastutil.objects.ObjectArrays} apply here.
 *
 * <p>
 * This class implements the bulk methods <code>removeElements()</code>, <code>addElements()</code>
 * and <code>getElements()</code> using high-performance system calls (e.g.,
 * {@link System#arraycopy(Object,int,Object,int,int) System.arraycopy()} instead of expensive
 * loops.
 *
 * @see java.util.ArrayList
 */
public class IdentityArrayList<K> extends AbstractObjectList<K>
    implements RandomAccess, Cloneable, java.io.Serializable {
  private static final long serialVersionUID = 449125332499184497L;

  /** The initial default capacity of an array list. */
  public static final int DEFAULT_INITIAL_CAPACITY = 16;

  /**
   * Whether the backing array was passed to <code>wrap()</code>. In this case, we must reallocate
   * with the same type of array.
   */
  protected final boolean wrapped;

  /** The backing array. */
  protected transient K[] a;

  /** The current actual size of the list (never greater than the backing-array length). */
  protected int size;

  private static final boolean ASSERTS = false;

  /**
   * Creates a new array list using a given array.
   *
   * <P>
   * This constructor is only meant to be used by the wrapping methods.
   *
   * @param a the array that will be used to back this array list.
   */
  @SuppressWarnings("unused")
  protected IdentityArrayList(final K[] a, boolean dummy) {
    this.a = a;
    wrapped = true;
  }

  /**
   * Creates a new array list with given capacity.
   *
   * @param capacity the initial capacity of the array list (may be 0).
   */
  @SuppressWarnings("unchecked")
  public IdentityArrayList(final int capacity) {
    if (capacity < 0) {
      throw new IllegalArgumentException("Initial capacity (" + capacity + ") is negative");
    }
    a = (K[]) new Object[capacity];
    wrapped = false;
  }

  /**
   * Creates a new array list with {@link #DEFAULT_INITIAL_CAPACITY} capacity.
   */
  public IdentityArrayList() {
    this(DEFAULT_INITIAL_CAPACITY);
  }

  /**
   * Creates a new array list and fills it with a given collection.
   *
   * @param c a collection that will be used to fill the array list.
   */
  public IdentityArrayList(final Collection<? extends K> c) {
    this(c.size());
    size = ObjectIterators.unwrap(c.iterator(), a);
  }

  /**
   * Creates a new array list and fills it with a given type-specific collection.
   *
   * @param c a type-specific collection that will be used to fill the array list.
   */
  public IdentityArrayList(final ObjectCollection<? extends K> c) {
    this(c.size());
    size = ObjectIterators.unwrap(c.iterator(), a);
  }

  /**
   * Creates a new array list and fills it with a given type-specific list.
   *
   * @param l a type-specific list that will be used to fill the array list.
   */
  public IdentityArrayList(final ObjectList<? extends K> l) {
    this(l.size());
    l.getElements(0, a, 0, size = l.size());
  }

  /**
   * Creates a new array list and fills it with the elements of a given array.
   *
   * @param a an array whose elements will be used to fill the array list.
   */
  public IdentityArrayList(final K[] a) {
    this(a, 0, a.length);
  }

  /**
   * Creates a new array list and fills it with the elements of a given array.
   *
   * @param a an array whose elements will be used to fill the array list.
   * @param offset the first element to use.
   * @param length the number of elements to use.
   */
  public IdentityArrayList(final K[] a, final int offset, final int length) {
    this(length);
    System.arraycopy(a, offset, this.a, 0, length);
    size = length;
  }

  /**
   * Creates a new array list and fills it with the elements returned by an iterator..
   *
   * @param i an iterator whose returned elements will fill the array list.
   */
  public IdentityArrayList(final Iterator<? extends K> i) {
    this();
    while (i.hasNext()) {
      add(i.next());
    }
  }

  /**
   * Creates a new array list and fills it with the elements returned by a type-specific iterator..
   *
   * @param i a type-specific iterator whose returned elements will fill the array list.
   */
  public IdentityArrayList(final ObjectIterator<? extends K> i) {
    this();
    while (i.hasNext()) {
      add(i.next());
    }
  }

  /**
   * Returns the backing array of this list.
   *
   * <P>
   * If this array list was created by wrapping a given array, it is guaranteed that the type of the
   * returned array will be the same. Otherwise, the returned array will be of type {@link Object
   * Object[]} (in spite of the declared return type).
   *
   * <strong>Warning</strong>: This behaviour may cause (unfathomable) run-time errors if a method
   * expects an array actually of type <code>K[]</code>, but this methods returns an array of type
   * {@link Object Object[]}.
   *
   * @return the backing array.
   */
  public K[] elements() {
    return a;
  }

  /**
   * Wraps a given array into an array list of given size.
   *
   * @param a an array to wrap.
   * @param length the length of the resulting array list.
   * @return a new array list of the given size, wrapping the given array.
   */
  public static <K> IdentityArrayList<K> wrap(final K[] a, final int length) {
    if (length > a.length) {
      throw new IllegalArgumentException("The specified length (" + length
          + ") is greater than the array size (" + a.length + ")");
    }
    final var l = new IdentityArrayList<K>(a, false);
    l.size = length;
    return l;
  }

  /**
   * Wraps a given array into an array list.
   *
   * @param a an array to wrap.
   * @return a new array list wrapping the given array.
   */
  public static <K> IdentityArrayList<K> wrap(final K[] a) {
    return wrap(a, a.length);
  }

  /**
   * Ensures that this array list can contain the given number of entries without resizing.
   *
   * @param capacity the new minimum capacity for this array list.
   */
  @SuppressWarnings("unchecked")
  public void ensureCapacity(final int capacity) {
    if (wrapped) {
      a = ObjectArrays.ensureCapacity(a, capacity, size);
    } else {
      if (capacity > a.length) {
        final var t = new Object[capacity];
        System.arraycopy(a, 0, t, 0, size);
        a = (K[]) t;
      }
    }
    assert !ASSERTS || size <= a.length;
  }

  /**
   * Grows this array list, ensuring that it can contain the given number of entries without
   * resizing, and in case enlarging it at least by a factor of two.
   *
   * @param capacity the new minimum capacity for this array list.
   */
  @SuppressWarnings("unchecked")
  private void grow(final int capacity) {
    if (wrapped) {
      a = ObjectArrays.grow(a, capacity, size);
    } else {
      if (capacity > a.length) {
        final var newLength = (int) Math
            .max(Math.min(2L * a.length, it.unimi.dsi.fastutil.Arrays.MAX_ARRAY_SIZE), capacity);
        final var t = new Object[newLength];
        System.arraycopy(a, 0, t, 0, size);
        a = (K[]) t;
      }
    }
    assert !ASSERTS || size <= a.length;
  }

  @Override
  public void add(final int index, final K k) {
    ensureIndex(index);
    grow(size + 1);
    if (index != size) {
      System.arraycopy(a, index, a, index + 1, size - index);
    }
    a[index] = k;
    size++;
    assert !ASSERTS || size <= a.length;
  }

  @Override
  public boolean add(final K k) {
    grow(size + 1);
    a[size++] = k;
    assert !ASSERTS || size <= a.length;
    return true;
  }

  @Override
  public K get(final int index) {
    if (index >= size) {
      throw new IndexOutOfBoundsException(
          "Index (" + index + ") is greater than or equal to list size (" + size + ")");
    }
    return a[index];
  }

  @Override
  public int indexOf(final Object k) {
    for (var i = 0; i < size; i++) {
      if (((k) == null ? (a[i]) == null : (k) == (a[i]))) {
        return i;
      }
    }
    return -1;
  }

  @Override
  public int lastIndexOf(final Object k) {
    for (var i = size; i-- != 0;) {
      if (((k) == null ? (a[i]) == null : (k) == (a[i]))) {
        return i;
      }
    }
    return -1;
  }

  @Override
  public K remove(final int index) {
    if (index >= size) {
      throw new IndexOutOfBoundsException(
          "Index (" + index + ") is greater than or equal to list size (" + size + ")");
    }
    final var old = a[index];
    size--;
    if (index != size) {
      System.arraycopy(a, index + 1, a, index, size - index);
    }
    a[size] = null;
    assert !ASSERTS || size <= a.length;
    return old;
  }

  public boolean rem(final Object k) {
    var index = indexOf(k);
    if (index == -1) {
      return false;
    }
    remove(index);
    assert !ASSERTS || size <= a.length;
    return true;
  }

  @Override
  public boolean remove(final Object o) {
    return rem(o);
  }

  @Override
  public K set(final int index, final K k) {
    if (index >= size) {
      throw new IndexOutOfBoundsException(
          "Index (" + index + ") is greater than or equal to list size (" + size + ")");
    }
    var old = a[index];
    a[index] = k;
    return old;
  }

  @Override
  public void clear() {
    Arrays.fill(a, 0, size, null);
    size = 0;
    assert !ASSERTS || size <= a.length;
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public void size(final int size) {
    if (size > a.length) {
      ensureCapacity(size);
    }
    if (size > this.size) {
      Arrays.fill(a, this.size, size, (null));
    } else {
      Arrays.fill(a, size, this.size, (null));
    }
    this.size = size;
  }

  @Override
  public boolean isEmpty() {
    return size == 0;
  }

  /**
   * Trims this array list so that the capacity is equal to the size.
   *
   * @see java.util.ArrayList#trimToSize()
   */
  public void trim() {
    trim(0);
  }

  /**
   * Trims the backing array if it is too large.
   *
   * If the current array length is smaller than or equal to <code>n</code>, this method does
   * nothing. Otherwise, it trims the array length to the maximum between <code>n</code> and
   * {@link #size()}.
   *
   * <P>
   * This method is useful when reusing lists. {@linkplain #clear() Clearing a list} leaves the
   * array length untouched. If you are reusing a list many times, you can call this method with a
   * typical size to avoid keeping around a very large array just because of a few large transient
   * lists.
   *
   * @param n the threshold for the trimming.
   */
  @SuppressWarnings("unchecked")
  public void trim(final int n) {
    // TODO: use Arrays.trim() and preserve type only if necessary
    if (n >= a.length || size == a.length) {
      return;
    }
    final var t = (K[]) new Object[Math.max(n, size)];
    System.arraycopy(a, 0, t, 0, size);
    a = t;
    assert !ASSERTS || size <= a.length;
  }

  /**
   * Copies element of this type-specific list into the given array using optimized system calls.
   *
   * @param from the start index (inclusive).
   * @param a the destination array.
   * @param offset the offset into the destination array where to store the first element copied.
   * @param length the number of elements to be copied.
   */
  @Override
  public void getElements(final int from, final Object[] a, final int offset, final int length) {
    ObjectArrays.ensureOffsetLength(a, offset, length);
    System.arraycopy(this.a, from, a, offset, length);
  }

  /**
   * Removes elements of this type-specific list using optimized system calls.
   *
   * @param from the start index (inclusive).
   * @param to the end index (exclusive).
   */
  @Override
  public void removeElements(final int from, final int to) {
    it.unimi.dsi.fastutil.Arrays.ensureFromTo(size, from, to);
    System.arraycopy(a, to, a, from, size - to);
    size -= (to - from);
    var i = to - from;
    while (i-- != 0) {
      a[size + i] = null;
    }
  }

  /**
   * Adds elements to this type-specific list using optimized system calls.
   *
   * @param index the index at which to add elements.
   * @param a the array containing the elements.
   * @param offset the offset of the first element to add.
   * @param length the number of elements to add.
   */
  @Override
  public void addElements(final int index, final K[] a, final int offset, final int length) {
    ensureIndex(index);
    ObjectArrays.ensureOffsetLength(a, offset, length);
    grow(size + length);
    System.arraycopy(this.a, index, this.a, index + length, size - index);
    System.arraycopy(a, offset, this.a, index, length);
    size += length;
  }

  @Override
  public ObjectListIterator<K> listIterator(final int index) {
    ensureIndex(index);
    return new AbstractObjectListIterator<K>() {
      int pos = index, last = -1;

      @Override
      public boolean hasNext() {
        return pos < size;
      }

      @Override
      public boolean hasPrevious() {
        return pos > 0;
      }

      @Override
      public K next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        return a[last = pos++];
      }

      @Override
      public K previous() {
        if (!hasPrevious()) {
          throw new NoSuchElementException();
        }
        return a[last = --pos];
      }

      @Override
      public int nextIndex() {
        return pos;
      }

      @Override
      public int previousIndex() {
        return pos - 1;
      }

      @Override
      public void add(K k) {
        if (last == -1) {
          throw new IllegalStateException();
        }
        IdentityArrayList.this.add(pos++, k);
        last = -1;
      }

      @Override
      public void set(K k) {
        if (last == -1) {
          throw new IllegalStateException();
        }
        IdentityArrayList.this.set(last, k);
      }

      @Override
      public void remove() {
        if (last == -1) {
          throw new IllegalStateException();
        }
        IdentityArrayList.this.remove(last);
        /*
         * If the last operation was a next(), we are removing an element *before* us, and we must
         * decrease pos correspondingly.
         */
        if (last < pos) {
          pos--;
        }
        last = -1;
      }
    };
  }

  @Override
  public IdentityArrayList<K> clone() {
    var c = new IdentityArrayList<K>(size);
    System.arraycopy(a, 0, c.a, 0, size);
    c.size = size;
    return c;
  }

  private boolean valEquals(final K a, final K b) {
    return a == b;
  }

  /**
   * Compares this type-specific array list to another one.
   *
   * <P>
   * This method exists only for sake of efficiency. The implementation inherited from the abstract
   * implementation would already work.
   *
   * @param l a type-specific array list.
   * @return true if the argument contains the same elements of this type-specific array list.
   */
  public boolean equals(final IdentityArrayList<K> l) {
    if (l == this) {
      return true;
    }
    var s = size();
    if (s != l.size()) {
      return false;
    }
    final var a1 = a;
    final var a2 = l.a;
    while (s-- != 0) {
      if (!valEquals(a1[s], a2[s])) {
        return false;
      }
    }
    return true;
  }

  /**
   * Compares this array list to another array list.
   *
   * <P>
   * This method exists only for sake of efficiency. The implementation inherited from the abstract
   * implementation would already work.
   *
   * @param l an array list.
   * @return a negative integer, zero, or a positive integer as this list is lexicographically less
   *         than, equal to, or greater than the argument.
   */
  @SuppressWarnings("unchecked")
  public int compareTo(final IdentityArrayList<? extends K> l) {
    final int s1 = size(), s2 = l.size();
    final K[] a1 = a, a2 = l.a;
    K e1, e2;
    int r, i;
    for (i = 0; i < s1 && i < s2; i++) {
      e1 = a1[i];
      e2 = a2[i];
      if ((r = (((Comparable<K>) (e1)).compareTo(e2))) != 0) {
        return r;
      }
    }
    return i < s2 ? -1 : (i < s1 ? 1 : 0);
  }

  private void writeObject(java.io.ObjectOutputStream s) throws java.io.IOException {
    s.defaultWriteObject();
    for (var i = 0; i < size; i++) {
      s.writeObject(a[i]);
    }
  }

  @SuppressWarnings("unchecked")
  private void readObject(java.io.ObjectInputStream s)
      throws java.io.IOException, ClassNotFoundException {
    s.defaultReadObject();
    a = (K[]) new Object[size];
    for (var i = 0; i < size; i++) {
      a[i] = (K) s.readObject();
    }
  }

  /**
   * For highest performance we sometimes must iterate over the actual Object array. This should
   * only be used in carefully controlled circumstances.
   */
  public Object[] getArrayRef() {
    return a;
  }

}
