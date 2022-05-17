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
package org.apache.geode.internal.tcp;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;

import org.apache.geode.internal.ByteBufferWriter;

/**
 * <p>
 * ByteBufferInputStream is an input stream for ByteBuffer objects. It's incredible that the jdk
 * doesn't have one of these already.
 * <p>
 *
 * The methods in this class throw BufferUnderflowException, not EOFException, if the end of the
 * buffer is reached before we read the full amount. That breaks the contract for InputStream and
 * DataInput, but it works for our code.
 *
 * @since GemFire 3.0
 */

public class ByteBufferInputStream extends InputStream
    implements DataInput, java.io.Externalizable {
  /**
   * This interface is used to wrap either a ByteBuffer as the source of bytes for a
   * ByteBufferInputStream.
   */
  public interface ByteSource {
    int position();

    int limit();

    int capacity();

    int remaining();

    void position(int newPosition);

    void limit(int endOffset);

    void get(byte[] b);

    void get(byte[] b, int off, int len);

    byte get();

    byte get(int pos);

    short getShort();

    short getShort(int pos);

    char getChar();

    char getChar(int pos);

    int getInt();

    int getInt(int pos);

    long getLong();

    long getLong(int pos);

    float getFloat();

    float getFloat(int pos);

    double getDouble();

    double getDouble(int pos);

    boolean hasArray();

    byte[] array();

    int arrayOffset();

    ByteSource duplicate();

    ByteSource slice(int length);

    ByteSource slice(int pos, int limit);

    /**
     * Returns the ByteBuffer that this ByteSource wraps; null if no ByteBuffer
     */
    ByteBuffer getBackingByteBuffer();

    void sendTo(ByteBuffer out);

    void sendTo(DataOutput out) throws IOException;
  }

  public static class ByteSourceFactory {
    public static ByteSource wrap(byte[] bytes) {
      return new ByteBufferByteSource(ByteBuffer.wrap(bytes));
    }

    public static ByteSource create(ByteBuffer bb) {
      return new ByteBufferByteSource(bb);
    }
  }

  public static class ByteBufferByteSource implements ByteSource {
    private final ByteBuffer bb;

    public ByteBufferByteSource(ByteBuffer bb) {
      this.bb = bb;
    }

    /**
     * Returns the current hash code of this byte source.
     *
     * <p>
     * The hash code of a byte source depends only upon its remaining elements; that is, upon the
     * elements from <tt>position()</tt> up to, and including, the element at
     * <tt>limit()</tt>&nbsp;-&nbsp;<tt>1</tt>.
     *
     * <p>
     * Because byte source hash codes are content-dependent, it is inadvisable to use byte sources
     * as keys in hash maps or similar data structures unless it is known that their contents will
     * not change.
     * <p>
     *
     * @return The current hash code of this byte source
     */
    @Override
    public int hashCode() {
      int h = 1;
      int p = position();
      for (int i = limit() - 1; i >= p; i--) {
        h = 31 * h + (int) get(i);
      }
      return h;
    }

    @Override
    public boolean equals(Object ob) {
      if (this == ob) {
        return true;
      }
      if (!(ob instanceof ByteSource)) {
        return false;
      }
      ByteSource that = (ByteSource) ob;
      if (remaining() != that.remaining()) {
        return false;
      }
      int p = position();
      for (int i = limit() - 1, j = that.limit() - 1; i >= p; i--, j--) {
        if (get(i) != that.get(j)) {
          return false;
        }
      }
      return true;
    }

    @Override
    public ByteSource duplicate() {
      return ByteSourceFactory.create(bb.duplicate());
    }

    @Override
    public byte get() {
      return bb.get();
    }

    @Override
    public void get(byte[] b, int off, int len) {
      bb.get(b, off, len);
    }

    @Override
    public int remaining() {
      return bb.remaining();
    }

    @Override
    public int position() {
      return bb.position();
    }

    @Override
    public byte get(int pos) {
      return bb.get(pos);
    }

    @Override
    public char getChar() {
      return bb.getChar();
    }

    @Override
    public char getChar(int pos) {
      return bb.getChar(pos);
    }

    @Override
    public double getDouble() {
      return bb.getDouble();
    }

    @Override
    public double getDouble(int pos) {
      return bb.getDouble(pos);
    }

    @Override
    public float getFloat() {
      return bb.getFloat();
    }

    @Override
    public float getFloat(int pos) {
      return bb.getFloat(pos);
    }

    @Override
    public void get(byte[] b) {
      bb.get(b);
    }

    @Override
    public int getInt() {
      return bb.getInt();
    }

    @Override
    public int getInt(int pos) {
      return bb.getInt(pos);
    }

    @Override
    public long getLong() {
      return bb.getLong();
    }

    @Override
    public long getLong(int pos) {
      return bb.getLong(pos);
    }

    @Override
    public short getShort() {
      return bb.getShort();
    }

    @Override
    public short getShort(int pos) {
      return bb.getShort(pos);
    }

    @Override
    public int limit() {
      return bb.limit();
    }

    @Override
    public void position(int newPosition) {
      bb.position(newPosition);
    }

    @Override
    public boolean hasArray() {
      return bb.hasArray();
    }

    @Override
    public byte[] array() {
      return bb.array();
    }

    @Override
    public int arrayOffset() {
      return bb.arrayOffset();
    }

    @Override
    public void limit(int endOffset) {
      bb.limit(endOffset);
    }

    @Override
    public ByteSource slice(int length) {
      if (length < 0) {
        throw new IllegalArgumentException();
      }
      ByteBuffer dup = bb.duplicate();
      dup.limit(dup.position() + length);
      return ByteSourceFactory.create(dup.slice());
    }

    @Override
    public ByteSource slice(int pos, int limit) {
      ByteBuffer dup = bb.duplicate();
      dup.limit(limit);
      dup.position(pos);
      return ByteSourceFactory.create(dup.slice());
    }

    @Override
    public int capacity() {
      return bb.capacity();
    }

    @Override
    public void sendTo(ByteBuffer out) {
      out.put(bb);
    }

    @Override
    public void sendTo(DataOutput out) throws IOException {
      int len = remaining();
      if (len == 0) {
        return;
      }
      if (out instanceof ByteBufferWriter) {
        ((ByteBufferWriter) out).write(bb);
        return;
      }
      if (bb.hasArray()) {
        byte[] bytes = bb.array();
        int offset = bb.arrayOffset() + bb.position();
        out.write(bytes, offset, len);
        bb.position(bb.limit());
      } else {
        while (len > 0) {
          out.writeByte(get());
          len--;
        }
      }
    }

    @Override
    public ByteBuffer getBackingByteBuffer() {
      return bb;
    }
  }

  private ByteSource buffer;

  public ByteBufferInputStream(ByteBuffer buffer) {
    setBuffer(buffer);
  }

  public ByteBufferInputStream() {}

  protected ByteBufferInputStream(ByteBufferInputStream copy) {
    buffer = copy.buffer.duplicate();
  }

  public void setBuffer(ByteSource buffer) {
    if (buffer == null) {
      throw new NullPointerException();
    }
    this.buffer = buffer;
  }

  public void setBuffer(ByteBuffer bb) {
    if (bb == null) {
      throw new NullPointerException();
    }
    setBuffer(ByteSourceFactory.create(bb));
  }

  /**
   * See the InputStream read method for javadocs. Note that if an attempt to read past the end of
   * the wrapped ByteBuffer is done this method throws BufferUnderflowException
   */
  @Override
  public int read() {
    return (buffer.get() & 0xff);
  }


  /*
   * this method is not thread safe See the InputStream read method for javadocs. Note that if an
   * attempt to read past the end of the wrapped ByteBuffer is done this method throws
   * BufferUnderflowException
   */
  @Override
  public int read(byte[] b, int off, int len) {
    buffer.get(b, off, len);
    return len;
  }

  @Override
  public int available() {
    return buffer.remaining();
  }

  public int position() {
    return buffer.position();
  }

  // GemFire does not use mark or reset so I changed this class
  // to just inherit from InputStream which does not support mark/reset.
  // That way we do not need to add support for them to the new ByteSource class.

  // @Override
  // public boolean markSupported() {
  // return true;
  // }
  //
  // @Override
  // public void mark(int limit) {
  // this.buffer.mark();
  // }
  //
  // @Override
  // public void reset() {
  // this.buffer.reset();
  // }

  @Override
  public long skip(long n) throws IOException {
    if (n <= Integer.MAX_VALUE) {
      return skipBytes((int) n);
    } else {
      return super.skip(n);
    }
  }

  @Override
  public boolean readBoolean() {
    return buffer.get() != 0;
  }

  public boolean readBoolean(int pos) {
    return buffer.get(pos) != 0;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.io.DataInput#readByte()
   */
  @Override
  public byte readByte() {
    return buffer.get();
  }

  public byte readByte(int pos) {
    return buffer.get(pos);
  }

  /*
   * (non-Javadoc)
   *
   * @see java.io.DataInput#readChar()
   */
  @Override
  public char readChar() {
    return buffer.getChar();
  }

  public char readChar(int pos) {
    return buffer.getChar(pos);
  }

  /*
   * (non-Javadoc)
   *
   * @see java.io.DataInput#readDouble()
   */
  @Override
  public double readDouble() {
    return buffer.getDouble();
  }

  public double readDouble(int pos) {
    return buffer.getDouble(pos);
  }

  /*
   * (non-Javadoc)
   *
   * @see java.io.DataInput#readFloat()
   */
  @Override
  public float readFloat() {
    return buffer.getFloat();
  }

  public float readFloat(int pos) {
    return buffer.getFloat(pos);
  }

  /*
   * (non-Javadoc)
   *
   * @see java.io.DataInput#readFully(byte[])
   */
  @Override
  public void readFully(byte[] b) {
    buffer.get(b);

  }

  /*
   * (non-Javadoc)
   *
   * @see java.io.DataInput#readFully(byte[], int, int)
   */
  @Override
  public void readFully(byte[] b, int off, int len) {
    buffer.get(b, off, len);

  }

  /*
   * (non-Javadoc)
   *
   * @see java.io.DataInput#readInt()
   */
  @Override
  public int readInt() {
    return buffer.getInt();
  }

  public int readInt(int pos) {
    return buffer.getInt(pos);
  }

  /*
   * (non-Javadoc)
   *
   * @see java.io.DataInput#readLine()
   */
  @Override
  public String readLine() {
    throw new UnsupportedOperationException();
  }

  /*
   * (non-Javadoc)
   *
   * @see java.io.DataInput#readLong()
   */
  @Override
  public long readLong() {
    return buffer.getLong();
  }

  public long readLong(int pos) {
    return buffer.getLong(pos);
  }

  /*
   * (non-Javadoc)
   *
   * @see java.io.DataInput#readShort()
   */
  @Override
  public short readShort() {
    return buffer.getShort();
  }

  public short readShort(int pos) {
    return buffer.getShort(pos);
  }

  /*
   * (non-Javadoc)
   *
   * @see java.io.DataInput#readUTF()
   */
  @Override
  public String readUTF() throws IOException {
    return DataInputStream.readUTF(this);
  }

  /*
   * (non-Javadoc)
   *
   * @see java.io.DataInput#readUnsignedByte()
   */
  @Override
  public int readUnsignedByte() {
    return buffer.get() & 0xff;
  }

  public int readUnsignedByte(int pos) {
    return buffer.get(pos) & 0xff;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.io.DataInput#readUnsignedShort()
   */
  @Override
  public int readUnsignedShort() {
    return buffer.getShort() & 0xffff;
  }

  public int readUnsignedShort(int pos) {
    return buffer.getShort(pos) & 0xffff;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.io.DataInput#skipBytes(int)
   */
  @Override
  public int skipBytes(int n) {
    int newPosition = buffer.position() + n;
    if (newPosition > buffer.limit()) {
      newPosition = buffer.limit();
      n = newPosition - buffer.position();
    }
    buffer.position(newPosition);
    return n;
  }

  public int size() {
    return buffer.limit();
  }

  public byte get(int idx) {
    return buffer.get(idx);
  }

  public short getShort(int idx) {
    return buffer.getShort(idx);
  }

  public int getInt(int idx) {
    return buffer.getInt(idx);
  }

  public void position(int absPos) {
    // if (absPos < 0) {
    // throw new IllegalArgumentException("position was less than zero " + absPos);
    // } else if (absPos > this.buffer.limit()) {
    // throw new IllegalArgumentException( "position " + absPos + " was greater than the limit " +
    // this.buffer.limit());
    // }
    buffer.position(absPos);
  }

  public void sendTo(DataOutput out) throws IOException {
    buffer.position(0);
    buffer.sendTo(out);
  }

  public void sendTo(ByteBuffer out) {
    buffer.position(0);
    buffer.sendTo(out);
  }

  public ByteSource slice(int length) {
    return buffer.slice(length);
  }

  public ByteSource slice(int startOffset, int endOffset) {
    return buffer.slice(startOffset, endOffset);
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeBoolean(buffer != null);
    if (buffer != null) {
      out.writeInt(buffer.capacity());
      out.writeInt(buffer.limit());
      out.writeInt(buffer.position());
      for (int i = 0; i < buffer.capacity(); i++) {
        out.write(buffer.get(i));
      }
    }
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    boolean hasBuffer = in.readBoolean();
    if (hasBuffer) {
      int capacity = in.readInt();
      int limit = in.readInt();
      int position = in.readInt();
      byte[] bytes = new byte[capacity];
      int bytesRead = in.read(bytes);
      if (bytesRead != capacity) {
        throw new IOException(
            "Expected to read " + capacity + " bytes but only read " + bytesRead + " bytes.");
      }
      setBuffer(ByteBuffer.wrap(bytes, position, limit - position));
    } else {
      buffer = null;
    }
  }

  public ByteSource getBuffer() {
    return buffer;
  }
}
