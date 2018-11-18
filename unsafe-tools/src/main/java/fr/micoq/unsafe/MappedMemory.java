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
package fr.micoq.unsafe;

import java.io.Closeable;
import java.io.EOFException;
import java.io.FileDescriptor;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.security.AccessController;
import java.security.PrivilegedAction;

/*
 * a true 64 (63 :p) bits memory-mapped file with madvise capacity
 */
public class MappedMemory implements Closeable {
    
  public static final int MADV_NORMAL = 0;
  public static final int MADV_SEQUENTIAL = 1;
  public static final int MADV_RANDOM = 2;
  public static final int MADV_WILLNEED = 3;
  public static final int MADV_DONTNEED = 4;
  
  public static final int FADV_NORMAL = 0;
  public static final int FADV_RANDOM = 1;
  public static final int FADV_SEQUENTIAL = 2;
  public static final int FADV_WILLNEED = 3;
  public static final int FADV_DONTNEED = 4;
  public static final int FADV_NOREUSE = 5;
  
  static {
    AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
      // libMappedMemory.so
      System.loadLibrary("mappedmemory");
      return null;
    });
  }
  
  private long length;
  private long addr;
  private boolean closed;
  private boolean dropCacheOnClose;
  private FileDescriptor fileDescriptor;
  
  private MappedMemory(long addr, long length, FileDescriptor fileDescriptor) {
    this.addr = addr;
    this.length = length;
    this.closed = false;
    this.fileDescriptor = fileDescriptor;
    this.dropCacheOnClose = true;
  }
  
  public static native MappedMemory mapFile(String path) throws IOException;
  
  private static native void madvise(long addr, long length, int advice) throws IOException;
  private static native void fadvise(FileDescriptor fd, long offset, long length, int advice) throws IOException;
  private static native int munmap(long addr, long length) throws IOException;
  private static native int pageSize();
  private static native void closeDescriptor(FileDescriptor fd) throws IOException;
  
  public byte getByte(long position) throws EOFException {
    if(position < 0)
      throw new BufferUnderflowException();
    if(position >= this.length)
      throw new EOFException("Read past EOF");
    if(this.closed)
      throw new EOFException("Cannot read a closed mapped memory");
    return getByteUnsafe(position);
  }
  
  public int getBytes(byte[] out, int outOffset, long position, int size) throws EOFException {
    if(out == null)
      throw new NullPointerException("Cannot copy to a null array");
    if(position < 0)
      throw new BufferUnderflowException();
    if(position >= this.length || this.length - position < size)
      throw new EOFException(String.format("Read past EOF, offset: %d, size: %d, buffer size: %d", position,size, this.length));
    if(this.closed)
      throw new EOFException("Cannot read a closed mapped memory");
    return getBytesUnsafe(out,outOffset,position,size);
  }
  
  public byte getByteUnsafe(long offset) {
    return UnsafeAccessor.getUnsafe().getByte(this.addr+offset);
  }
  
  public int getBytesUnsafe(byte[] out, int outOffset, long offset, int size) {
    int read = Math.min(size,out.length-outOffset);
    // We can't make a view/slice here so we don't have a choice to copy
    UnsafeAccessor.getUnsafe().copyMemory(null, this.addr+offset, out, UnsafeAccessor.ARRAY_BYTE_BASE_OFFSET+outOffset, read);
    return read;
  }
  
  public void madviseDefault() throws IOException {
    if(this.closed || this.addr == 0)
      return;
    MappedMemory.madvise(this.addr, this.length, MappedMemory.MADV_NORMAL);
  }
  
  public void madviseRandom() throws IOException {
    if(this.closed || this.addr == 0)
      return;
    MappedMemory.madvise(this.addr, this.length, MappedMemory.MADV_RANDOM);
  }
  
  public void madviseSequential() throws IOException {
    if(this.closed || this.addr == 0)
      return;
    MappedMemory.madvise(this.addr, this.length, MappedMemory.MADV_SEQUENTIAL);
  }
  
  public void preload() throws IOException {
    if(this.closed || this.addr == 0)
      return;
    MappedMemory.madvise(this.addr, this.length, MappedMemory.MADV_WILLNEED);
    long pos = 0;
    int pageSize = pageSize();
    while(pos < this.length) {
      getByteUnsafe(pos);
      pos += pageSize;
    }
  }
  
  public void close() {
    if(this.closed)
      return;
    if(this.addr != 0) {
      try {
        MappedMemory.munmap(this.addr, this.length);
      } catch (IOException e) {
        // ignore
      }
    }
    if(this.dropCacheOnClose) {
      try {
        MappedMemory.fadvise(this.fileDescriptor, 0, 0, MappedMemory.FADV_DONTNEED);
      } catch (IOException e) {
        // ignore
      }
    }
    try {
      MappedMemory.closeDescriptor(this.fileDescriptor);
    } catch (IOException e) {
      // ignore
    }
    this.closed = true;
  }
  
  @Override
  public void finalize() {
    this.close();
  }

  public long getLength() {
    return this.length;
  }
}
