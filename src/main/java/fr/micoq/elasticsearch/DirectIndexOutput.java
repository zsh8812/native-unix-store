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
package fr.micoq.elasticsearch;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.zip.CRC32;
import java.util.zip.CheckedOutputStream;

import org.apache.lucene.store.IndexOutput;

class DirectIndexOutput extends IndexOutput {
  private final CRC32 crc = new CRC32();
  private final OutputStream os;
  private long bytesWritten = 0L;
  private boolean flushedOnClose = false;

  DirectIndexOutput(Path path, int bufferSize) throws IOException {
    super("DirectIndexOutput(path=\"" + path.toString() + "\")", path.getFileName().toString());
    this.os = new CheckedOutputStream(new DirectOutputStream(path,bufferSize),crc); 
  }
  
  @Override
  public final void writeByte(byte b) throws IOException {
    os.write(b);
    bytesWritten++;
  }
  
  @Override
  public final void writeBytes(byte[] b, int offset, int length) throws IOException {
    os.write(b, offset, length);
    bytesWritten += length;
  }

  @Override
  public long getFilePointer() {
    return bytesWritten;
  }

  @Override
  public long getChecksum() throws IOException {
    os.flush();
    return crc.getValue();
  }
  
  @Override
  public void close() throws IOException {
    try (OutputStream o = os) {
      // We want to make sure that os.flush() was running before close:
      // BufferedOutputStream may ignore IOExceptions while flushing on close().
      // We keep this also in Java 8, although it claims to be fixed there,
      // because there are more bugs around this! See:
      // # https://bugs.openjdk.java.net/browse/JDK-7015589
      // # https://bugs.openjdk.java.net/browse/JDK-8054565
      if (!flushedOnClose) {
        flushedOnClose = true; // set this BEFORE calling flush!
        o.flush();
      }
    }
  }
}
