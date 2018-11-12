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

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import fr.micq.unsafe.DirectBufferTools;
import fr.micq.unsafe.DirectIO;

public final class DirectOutputStream extends OutputStream {
  
  private FileOutputStream fos;
  private final FileChannel channel;
  private long realFileLength;
  private boolean isOpen;
  private ByteBuffer buffer;
  private int bufferSize;
  
  public DirectOutputStream(Path path, int bufferSize) throws IOException {
    try {
      this.fos = AccessController.doPrivileged((PrivilegedExceptionAction<FileOutputStream>) () -> {
        return new FileOutputStream(DirectIO.openDirect(path.toString(), false));
      });
    } catch (PrivilegedActionException e) {
      throw (IOException) e.getException();
    }
    this.buffer = null;
    this.channel = fos.getChannel();
    this.realFileLength = 0L;
    this.bufferSize = bufferSize;
    this.isOpen = true;
    this.buffer = null;
  }
  
  // Lazy init
  private void requireBuffer() {
    if(this.buffer == null) {
      this.buffer = DirectBufferTools.allocateAlignedByteBuffer(this.bufferSize);
    }
  }

  @Override
  public void write(int b) throws IOException {
    requireBuffer();
    this.buffer.put((byte) b);
    this.realFileLength++;
    if (this.buffer.position() == this.buffer.capacity()) {
      writeBuffer();
    }
  }
  
  @Override
  public void write(byte b[], int off, int len) throws IOException {
    if (b == null) {
      throw new NullPointerException();
    }
    else if ((off < 0) || (off > b.length) || (len < 0) || ((off + len) > b.length) || ((off + len) < 0)) {
      throw new IndexOutOfBoundsException();
    }
    else if (len == 0) {
      return;
    }
    requireBuffer();
    int remain = this.buffer.capacity() - this.buffer.position();
    while(len >= remain) {
      this.buffer.put(b,off,remain);
      this.realFileLength += remain;
      off += remain;
      len -= remain;
      remain = this.buffer.capacity();
      writeBuffer();
    }
    if(len > 0) {
      this.buffer.put(b,off,len);
      this.realFileLength += len;
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
    if (this.isOpen) {
      this.isOpen = false;
      try {
        writeBuffer();
      } finally {
        try {
          this.channel.truncate(realFileLength);
        } finally {
          try {
            this.channel.close();
          } finally {
            try {
              this.fos.close();
            }
            finally {
              closeBuffer();
            }
          }
        }
      }
    }
  }
  
  private void writeBuffer() throws IOException {
    requireBuffer();
    ((java.nio.Buffer)this.buffer).rewind();
    // TODO memset here to avoid remaining garbage after the data before truncation ?
    this.channel.write(buffer);
    ((java.nio.Buffer)this.buffer).clear();
  }
  
}
