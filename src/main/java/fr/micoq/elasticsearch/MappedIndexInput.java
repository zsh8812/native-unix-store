/*
 * Copyright 2018 Michaël Coquard
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package fr.micq.elasticsearch;

import java.io.EOFException;
import java.io.IOException;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;

import fr.micq.unsafe.MappedMemory;

public class MappedIndexInput extends IndexInput implements RandomAccessInput {
  private MappedMemory memory;
  private long length;
  private long pos;
  private long offset;
  private MappedIndexInput parent;
  private MappedIndexInputGuard guard;
  
  private MappedIndexInput(String resourceDescription,MappedIndexInputGuard guard, long offset, long length, MappedIndexInput parent) {
    super(resourceDescription);
    this.memory = guard.getMemory();
    this.length = length;
    this.guard = guard;
    this.offset = offset;
    this.pos = 0;
    this.parent = parent; // Lucene ensure it never call close() on the main slice before accessing to the child slices
    if(parent == null)
      guard.open();
  }
  
  private MappedIndexInput(MappedIndexInput in) {
    super(in.toString());
    this.memory = in.memory;
    this.length = in.length;
    this.guard = in.guard;
    this.offset = in.offset;
    this.pos = in.pos;
    this.parent = in;
  }
  
  public static MappedIndexInput makeInput(String resourceDescription, MappedIndexInputGuard guard) {
    return new MappedIndexInput(resourceDescription, guard, 0, guard.getMemory().getLength(),null);
  }

  @Override
  public void close() throws IOException {
    if(parent == null)
      guard.close();
  }

  @Override
  public long getFilePointer() {
    return this.pos;
  }

  @Override
  public long length() {
    return this.length;
  }

  @Override
  public void seek(long pos) throws IOException {
    if(pos < 0)
      throw new IllegalArgumentException("The new position cannot be a negative value");
    // seek exactly after the last byte is allowed (even there is no data here)
    if(pos > this.length)
      throw new EOFException(String.format("Reached EOF, wanted position: %d, current slice length: %d",pos,this.length));
    this.pos = pos;
  }

  @Override
  public IndexInput slice(String sliceDescription, long pos, long length) throws IOException {
    if(pos < 0)
      throw new IllegalArgumentException("The slice position cannot be a negative value");
    if(pos >= this.length)
      throw new EOFException(String.format("Reached EOF, wanted position: %d, current slice length: %d",pos,this.length));
    if(this.length - pos < length)
      throw new IllegalArgumentException(String.format("Slice past EOF, wanted offset: %d, slice length: %d, outer slice length: %d",
          pos,length,this.length));
    String resourceDescription = null;
    if(sliceDescription != null) {
      resourceDescription = String.format("[slice=%s]",sliceDescription);
    }
    return new MappedIndexInput(resourceDescription, this.guard, this.offset + pos, length, this);
  }

  @Override
  public byte readByte() throws IOException {
    return readByte(this.pos++);
  }
  
  @Override
  public byte readByte(long pos) throws IOException {
    if(pos < 0)
      throw new IllegalArgumentException("The position cannot be a negative value");
    if(pos >= this.length)
      throw new EOFException(String.format("Read past EOF, wanted position: %d, current slice length: %d",this.pos,this.length));
    return this.memory.getByteUnsafe(this.offset + pos);
  }

  @Override
  public void readBytes(byte[] b, int offset, int len) throws IOException {
    this.pos += readBytes(b,offset,len,this.pos);
  }
  
  // clone() is used in merges (to calculate the checksum of the output segment in CodecUtil.checksumEntireFile())
  @Override
  public IndexInput clone() {
    return new MappedIndexInput(this);
  }
  
  private int readBytes(byte[] b, int offset, int len, long pos) throws IOException {
    // Sometimes, Lucene calls this method to read 0 bytes at the end of the slice, so we can't fail here
    if(len == 0)
      return 0;
    if(b == null)
      throw new NullPointerException("Cannot copy to a null array");
    if(pos < 0)
      throw new IllegalArgumentException("The position cannot be a negative value");
    if(pos >= this.length || this.length - pos < len)
      throw new EOFException(String.format("Read past EOF, wanted position: %d, current slice length: %d, bytes to read: %d",
          pos,this.length,len));
    return this.memory.getBytesUnsafe(b, offset, this.offset + pos, len);
  }

  // Random access (absolute position)
  
  //TODO : better implementation than seek() ?
  @Override
  public int readInt(long pos) throws IOException {
    long old = this.pos;
    seek(pos);
    int result =  super.readInt();
    seek(old); //TODO : needeed ?
    return result;
  }

  @Override
  public long readLong(long pos) throws IOException {
    long old = this.pos;
    seek(pos);
    long result =  super.readLong();
    seek(old); //TODO : needeed ?
    return result;
  }

  @Override
  public short readShort(long pos) throws IOException {
    long old = this.pos;
    seek(pos);
    short result =  super.readShort();
    seek(old); //TODO : needeed ?
    return result;
  }
  
}
