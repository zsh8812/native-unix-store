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
package fr.micq.unsafe;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

public class DirectBufferTools {
  
  private static final Method getCleaner, getAddress, getBaseBuffer;
  
  static {
    Class<?> directBufferClass;
    try {
      directBufferClass = Class.forName("java.nio.DirectByteBuffer");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    try {
      getAddress = AccessController.doPrivileged((PrivilegedExceptionAction<Method>) () -> {
        Method method = directBufferClass.getMethod("address");
        method.setAccessible(true);
        return method;
      });
    } catch (PrivilegedActionException e) {
      throw new RuntimeException((Exception) e.getException());
    }
    try {
      getCleaner = AccessController.doPrivileged((PrivilegedExceptionAction<Method>) () -> {
        Method method = directBufferClass.getMethod("cleaner");
        method.setAccessible(true);
        return method;
      });
    } catch (PrivilegedActionException e) {
      throw new RuntimeException((Exception) e.getException());
    }
    try {
      getBaseBuffer = AccessController.doPrivileged((PrivilegedExceptionAction<Method>) () -> {
        Method method;
        try {
          method = directBufferClass.getMethod("attachment");
        } catch (NoSuchMethodException e) {
          method = directBufferClass.getMethod("viewedBuffer");
        }
        method.setAccessible(true);
        return method;
      });
    } catch (PrivilegedActionException e) {
      throw new RuntimeException((Exception) e.getException());
    }
  }
  
  /*
   * Compact version of https://gist.github.com/nitsanw
   * 
   * Allocate a direct buffer aligned on the system page size so it would
   * be sufficient to be used for direct I/O access.
   * The initial limit will be aligned too.
   */
  public static ByteBuffer allocateAlignedByteBuffer(int minCapacity) { 
    int align = UnsafeAccessor.getUnsafe().pageSize();
    int mask = align - 1;
    ByteBuffer buffy = ByteBuffer.allocateDirect(minCapacity + (align << 1));
    // Since java 9, we cannot cast the buffer directly into DirectBuffer, we must use reflexion
    //long address = ((DirectBuffer)buffy).address();
    long address = 0;
    try {
      address = (Long)getAddress.invoke(buffy);
    } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      // Never happens :)
    }
    int newPosition = (int)((align - (address & mask)) & mask);
    int newLimit = newPosition + minCapacity + ((align - (minCapacity & mask)) & mask);
    // Cast needeed to avoid a bug when generated Java 8 bytecode on a Java 9+ JVM (see https://github.com/apache/felix/pull/114)
    ((java.nio.Buffer)buffy).position(newPosition);
    ((java.nio.Buffer)buffy).limit(newLimit);
    return buffy.slice().order(ByteOrder.nativeOrder());
  }
  
  public static void freeBuffer(ByteBuffer buffer) {
    if(!buffer.isDirect()) {
      return;
    }
    try {
      Object baseBuffer = getBaseBuffer.invoke(buffer);
      if(baseBuffer != null) {
        buffer = (ByteBuffer) baseBuffer;
      }
      Object cleaner = getCleaner.invoke(buffer);
      cleaner.getClass().getMethod("clean").invoke(cleaner);
    } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      // Never happens :)
    } catch (NoSuchMethodException | SecurityException e) {
      // If clean() doesn't exists
    }
  }

}
