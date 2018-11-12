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
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Deque;
import java.util.LinkedList;

import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.IndexInput;

import fr.micq.unsafe.DirectBufferTools;
import fr.micq.unsafe.DirectIO;

final class DirectIndexInput extends IndexInput {

  private FileInputStream fis;
  private FileChannel channel;
  private int bufferSize;
  
  private boolean uptodate;
  private boolean cloned;
  private long pos;
  private long bufferPos;
  private ByteBuffer buffer;
  
  private Deque<DirectIndexInput> clones;
  
  DirectIndexInput(Path path, int bufferSize) throws IOException {
    super("DirectIndexInput(path=\"" + path + "\")");
    this.clones = new LinkedList<DirectIndexInput>();
    try {
      this.fis = AccessController.doPrivileged((PrivilegedExceptionAction<FileInputStream>) () -> {
        return new FileInputStream(DirectIO.openDirect(path.toString(), true));
      });
    } catch (PrivilegedActionException e) {
      throw (IOException) e.getException();
    }
    this.channel = fis.getChannel();
    this.cloned = false;
    this.buffer = null;
    this.bufferSize = bufferSize;
    this.bufferPos = 0L;
    this.pos = 0L;
    invalidate();
  }

  // for clone
  private DirectIndexInput(DirectIndexInput other) throws IOException {
    super(other.toString());
    this.fis = other.fis;
    this.channel = other.channel;
    this.cloned = true;
    this.buffer = null;
    this.clones = other.clones; // Needeed for chained clones
    this.clones.add(this);
    this.bufferSize = other.bufferSize;
    this.bufferPos = other.bufferPos;
    this.pos = other.pos;
  }
  
  // Lazy init
  private void requireBuffer() {
    if(this.buffer == null) {
      this.buffer = DirectBufferTools.allocateAlignedByteBuffer(this.bufferSize);
      this.bufferSize = this.buffer.capacity();
    }
  }
  
  private void closeBuffer() {
    if(this.buffer != null) {
      DirectBufferTools.freeBuffer(this.buffer);
      this.buffer = null;
    }
  }

  @Override
  public void close() throws IOException {
    if(!this.cloned) {
      try {
        this.channel.close();
      } finally {
        try {
          this.fis.close();
        }
        finally {
          closeBuffer();
          // We assume child buffers will not be used anymore
          for(DirectIndexInput clone : this.clones ) {
            clone.closeBuffer();
          }
          this.clones.clear();
        }
      }
    }
  }

  @Override
  public void seek(long pos) throws IOException {
    if(pos > length())
      throw new EOFException("Reached EOF");
    this.pos = pos;
    if(pos < this.bufferPos || pos >= this.bufferPos + this.bufferSize) {
      invalidate();
    } else {
      requireBuffer();
      ((java.nio.Buffer)this.buffer).position((int)(pos % this.bufferSize));
    }
  }

  @Override
  public byte readByte() throws IOException {
    refill();
    byte b;
    try {
      b = this.buffer.get();
    } catch (BufferUnderflowException ex) {
      throw new EOFException();
    }
    this.pos++;
    if(this.buffer.position() == this.buffer.limit()) {
    invalidate();
    }
    return b;
  }
  
  @Override
  public void readBytes(byte[] dst, int offset, int len) throws IOException {
  if(len == 0)
  return;
    while(true) {
    refill();
      int left = this.buffer.limit() - this.buffer.position();
      if(left == 0) { // some data remaining
        throw new EOFException("Read past EOF");
      } else if (left < len) { // not enough bytes in the buffer -> readahead
        this.buffer.get(dst, offset, left);
        this.pos += left;
        len -= left;
        offset += left;
        invalidate();
      } else { // enough bytes in the buffer
        this.buffer.get(dst, offset, len);
        this.pos += len;
        if(this.buffer.position() == this.buffer.limit()) { // just enough bytes in the buffer !
          invalidate();
        }
        break;
      }
    }
  }
  
  private void invalidate() {
    this.uptodate = false;
  }

  private void refill() throws IOException {
    if(this.uptodate)
      return;
    requireBuffer();
    final long newBufferPos = (this.pos / this.bufferSize) * this.bufferSize;
    ((java.nio.Buffer)this.buffer).clear();
    if(newBufferPos < length()) {
      int n = this.channel.read(this.buffer, newBufferPos);
      if (n < 0) {
        throw new EOFException("Attempt to read past EOF: " + this);
      }
      ((java.nio.Buffer)this.buffer).limit(n); // the buffer limit must be set manually
    } else {
      ((java.nio.Buffer)this.buffer).limit(0);
    }
    int newPosInbufferSize = (int)(pos % this.bufferSize);
    try {
      ((java.nio.Buffer)this.buffer).rewind().position(newPosInbufferSize);
    } catch (IllegalArgumentException ex) {
    // for the last buffer
      throw new IOException(ex);
    }
    this.bufferPos = newBufferPos;
    this.uptodate = true;
  }
  
  @Override
  public long getFilePointer() {
    return this.pos;
  }
  
  @Override
  public long length() {
    try {
      return channel.size();
    } catch (IOException ex) {
      throw new RuntimeException("IOException during length(): " + this, ex);
    }
  }

  @Override
  public DirectIndexInput clone() {
    try {
      return new DirectIndexInput(this);
    } catch (IOException ioe) {
      throw new RuntimeException("IOException during clone: " + this, ioe);
    }
  }

  @Override
  public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
    return BufferedIndexInput.wrap(sliceDescription, this, offset, length);
  }
  
}
