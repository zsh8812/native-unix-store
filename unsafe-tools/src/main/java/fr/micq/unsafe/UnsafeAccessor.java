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

import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import sun.misc.Unsafe;

public class UnsafeAccessor {
  private static Object unsafe;
  
  public static int ARRAY_BYTE_BASE_OFFSET;
  
  static {
    try {
      unsafe = AccessController.doPrivileged((PrivilegedExceptionAction<Object>) () -> {
        final Field f = Unsafe.class.getDeclaredField("theUnsafe");
        f.setAccessible(true);
        unsafe = (Unsafe)f.get(null); 
        ARRAY_BYTE_BASE_OFFSET = Unsafe.ARRAY_BYTE_BASE_OFFSET;
        return unsafe;
      });
    } catch (PrivilegedActionException e) {
      throw new RuntimeException((Exception) e.getException());
    }
  }
  
  public static Unsafe getUnsafe() {
    return (Unsafe)unsafe;
  }
}
