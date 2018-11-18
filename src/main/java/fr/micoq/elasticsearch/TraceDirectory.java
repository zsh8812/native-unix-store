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

// Like strace... for Lucene :)

import java.io.IOException;
import java.util.Collection;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IOContext.Context;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;

public class TraceDirectory extends Directory {

  private Directory delegate;
  private boolean dontdelete;
  private boolean traceData;

  private String contextToString(IOContext context) 
  {
    String iocon = null;
    String con = null;
    if(context == IOContext.READ) {
      iocon = "READ";
    } else if (context == IOContext.DEFAULT) {
      iocon = "DEFAULT";
    } else if (context == IOContext.READONCE) {
      iocon = "READONCE";
    } else {
      iocon = "other";
    }
    if(context.context == Context.DEFAULT) {
      con = "DEFAULT";
    } else if(context.context == Context.FLUSH) {
      con = "FLUSH";
    } else if(context.context == Context.MERGE) {
      con = "MERGE";
    } else if(context.context == Context.READ) {
      con = "READ";
    } else {
      con = "other";
    }
    return String.format("%s/%s",iocon,con);
  }
  
  public TraceDirectory(Directory delegate) {
    this(delegate,false,false);
  }
  
  public TraceDirectory(Directory delegate, boolean dontdelete, boolean traceData) {
    this.delegate = delegate;
    this.dontdelete = dontdelete;
    this.traceData = traceData;
  }
  
  @Override
  public void close() throws IOException {
    System.out.println(String.format("[%s,%s] Directory.close()",
        Thread.currentThread().getName(),this.toString()));
    this.delegate.close();
  }
  
  @Override
  public IndexOutput createOutput(String name, IOContext context) throws IOException {
    System.out.println(String.format("[%s,%s] Directory.createOutput(%s,%s)",
        Thread.currentThread().getName(),this.toString(),name,contextToString(context)));
    String resourceDescription = name;
    return new TraceIndexOutput(resourceDescription, name, this.delegate.createOutput(name,context));
  }
  
  @Override
  public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
    System.out.println(String.format("[%s,%s] Directory.createTempOutput()",
        Thread.currentThread().getName(),this.toString(),contextToString(context)));
    return new TraceIndexOutput(prefix+suffix, prefix, this.delegate.createTempOutput(prefix,suffix,context));
  }
  
  @Override
  public void deleteFile(String name) throws IOException {
    System.out.println(String.format("[%s,%s] Directory.deleteFile(%s)",
        Thread.currentThread().getName(),this.toString(),name));
    if(!this.dontdelete)
      this.delegate.deleteFile(name);
  }
  
  @Override
  public long fileLength(String name) throws IOException {
    long l = this.delegate.fileLength(name);
    System.out.println(String.format("[%s,%s] Directory.fileLength(%s) = %d",
        Thread.currentThread().getName(),this.toString(),name,l));
    return l;
  }
  
  @Override
  public String[] listAll() throws IOException {
    System.out.println(String.format("[%s,%s] Directory.listAll()",
        Thread.currentThread().getName(),this.toString()));
    return this.delegate.listAll();
  }
  
  @Override
  public Lock obtainLock(String name) throws IOException {
    System.out.println(String.format("[%s,%s] Directory.obtainLock(%s)",
        Thread.currentThread().getName(),this.toString(),name));
    return new TraceLock(delegate.obtainLock(name));
  }
  
  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    System.out.println(String.format("[%s,%s] Directory.openInput(%s,%s)",
        Thread.currentThread().getName(),this.toString(),name,contextToString(context)));
    return new TraceIndexInput(name,this.delegate.openInput(name, context));
  }
  
  @Override
  public void rename(String fromName, String toName) throws IOException {
    System.out.println(String.format("[%s,%s] Directory.rename(%s,%s)",
        Thread.currentThread().getName(),this.toString(),fromName,toName));
    this.delegate.rename(fromName,toName);
  }
  
  @Override
  public void sync(Collection<String> files) throws IOException {
    System.out.println(String.format("[%s,%s] Directory.sync()",
        Thread.currentThread().getName(),this.toString()));
    this.delegate.sync(files);
  }
  
  @Override
  public void syncMetaData() throws IOException {
    System.out.println(String.format("[%s,%s] Directory.syncMetaData()",
        Thread.currentThread().getName(),this.toString()));
    this.delegate.syncMetaData();
  }
  
  private class TraceIndexOutput extends IndexOutput {
    private IndexOutput delegateOutput;
    
    protected TraceIndexOutput(String resourceDescription, String name, IndexOutput delegateOutput) {
      super(resourceDescription, name);
      this.delegateOutput = delegateOutput;
    }
    
    @Override
    public void close() throws IOException {
      System.out.println(String.format("[%s,%s] IndexOutput.close()",
          Thread.currentThread().getName(),this.toString()));
      this.delegateOutput.close();
    }
    
    @Override
    public long getChecksum() throws IOException {
      long c = this.delegateOutput.getChecksum();
      System.out.println(String.format("[%s,%s] IndexOutput.getChecksum() = 0x%016x",
          Thread.currentThread().getName(),this.toString(),c));
      return c;
    }
    
    @Override
    public long getFilePointer() {
      long p = this.delegateOutput.getFilePointer(); 
      if(traceData)
        System.out.println(String.format("[%s,%s] IndexOutput.getFilePointer() = %d",
            Thread.currentThread().getName(),this.toString(),p));
      return p;
    }
    
    @Override
    public void writeByte(byte b) throws IOException {
      if(traceData)
        System.out.println(String.format("[%s,%s] IndexOutput.writeByte(0x%02x)",
            Thread.currentThread().getName(),this.toString(),b));
      this.delegateOutput.writeByte(b);
    }
  
    @Override
    public void writeBytes(byte[] data, int offset, int length) throws IOException {
      if(traceData)
        System.out.println(String.format("[%s,%s] IndexOutput.writeBytes(data[%d] @ %d)",
            Thread.currentThread().getName(),this.toString(),length,offset));
      this.delegateOutput.writeBytes(data, offset, length);
    }
  }

  private class TraceIndexInput extends IndexInput {
    private IndexInput delegateInput;
    
    protected TraceIndexInput(String resourceDescription, IndexInput delegateInput) {
      super(resourceDescription);
      this.delegateInput = delegateInput;
    }
    
    @Override
    public void readBytes(byte[] data, int offset, int length) throws IOException {
      if(traceData)
        System.out.println(String.format("[%s,%s] IndexInput.readBytes(data[%d] @ %d)",
            Thread.currentThread().getName(),this.toString(),length,offset));
      this.delegateInput.readBytes(data,offset,length);
    }
    
    @Override
    public byte readByte() throws IOException {
      byte b = this.delegateInput.readByte();
      if(traceData)
        System.out.println(String.format("[%s,%s] IndexInput.readByte() = 0x%02x",
            Thread.currentThread().getName(),this.toString(),b));
      return b;
    }
    
    @Override
    public IndexInput clone() {
      System.out.println(String.format("[%s,%s] IndexInput.clone()",
          Thread.currentThread().getName(),this.toString()));
      return delegateInput.clone();
    }
    
    @Override
    public IndexInput slice(String name, long offset, long length) throws IOException {
      System.out.println(String.format("[%s,%s] IndexInput.slice(name=%s @ %d, length=%d)",
          Thread.currentThread().getName(),this.toString(),name,offset,length));
      return this.delegateInput.slice(name, offset, length);
    }
    
    @Override
    public void seek(long offset) throws IOException {
      if(traceData)
        System.out.println(String.format("[%s,%s] IndexInput.seek(%d)",
            Thread.currentThread().getName(),this.toString(),offset));
      this.delegateInput.seek(offset);
    }
    
    @Override
    public long length() {
      long l = this.delegateInput.length();
      System.out.println(String.format("[%s,%s] IndexInput.length() = %d",
          Thread.currentThread().getName(),this.toString(),l));
      return l;
    }
    
    @Override
    public long getFilePointer() {
      long p = this.delegateInput.getFilePointer();
      if(traceData)
        System.out.println(String.format("[%s,%s] IndexInput.getFilePointer() = %d",
            Thread.currentThread().getName(),this.toString(),p));
      return p;
    }
    
    @Override
    public void close() throws IOException {
      System.out.println(String.format("[%s,%s] IndexInput.close()",
          Thread.currentThread().getName(),this.toString()));
      this.delegateInput.close();
    }
  }
  
  private class TraceLock extends Lock {
  
    private Lock delegateLock;
    
    TraceLock(Lock delegateLock) {
      this.delegateLock = delegateLock;
    }
    
    @Override
    public void close() throws IOException {
      System.out.println("Lock.close()");
      this.delegateLock.close();
    }
    
    @Override
    public void ensureValid() throws IOException {
      System.out.println("Lock.ensureValid()");
      this.delegateLock.ensureValid();
    }
  }
}
