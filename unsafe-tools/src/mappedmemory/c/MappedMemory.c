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
 
/*
 * madvise() and fadvise() code are taken from the original Lucene project (under Apache License 2.0)
 */

// WARNING: only for x86_64 arch !
 
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <jni.h>
#include <fcntl.h>     // posix_fadvise, constants for open
#include <string.h>    // strerror
#include <errno.h>     // errno
#include <sys/mman.h>  // mmap64, munmap, madvise
#include <sys/types.h> // constants for open
#include <sys/stat.h>  // constants for open
#include <unistd.h>    // getpagesize, close

/*
 * Class:     fr_micoq_unsafe_MappedMemory
 * Method:    pageSize
 * Signature: ()V
 */
JNIEXPORT jint JNICALL Java_fr_micoq_unsafe_MappedMemory_pageSize(JNIEnv *env, jclass _ignore)
{
  return getpagesize();
}

/*
 * Class:     fr_micoq_unsafe_MappedMemory
 * Method:    fadvise
 * Signature: (Ljava/io/FileDescriptor;JJI)V
 */
JNIEXPORT jint JNICALL Java_fr_micoq_unsafe_MappedMemory_fadvise(JNIEnv *env, jclass _ignore, jobject fileDescriptor, jlong offset, jlong length, jint advice)
{
  jclass class_ioex, class_fdesc;
  
  class_ioex = (*env)->FindClass(env, "java/io/IOException");
  if (class_ioex == NULL) {
    return -1;
  }

  class_fdesc = (*env)->FindClass(env, "java/io/FileDescriptor");
  if (class_fdesc == NULL) {
    return -1;
  }

  jfieldID fdField = (*env)->GetFieldID(env, class_fdesc, "fd", "I");
  if (fdField == NULL) {
    return -1;
  }

  int fd = (*env)->GetIntField(env, fileDescriptor, fdField);

  int osAdvice;
  switch(advice) {
    case 0:
      osAdvice = POSIX_FADV_NORMAL;
      break;
    case 1:
      osAdvice = POSIX_FADV_RANDOM;
      break;
    case 2:
      osAdvice = POSIX_FADV_SEQUENTIAL; // Doesn't evict pages on Linux (just double the readahead size)
      break;
    case 3:
      osAdvice = POSIX_FADV_WILLNEED;
      break;
    case 4:
      osAdvice = POSIX_FADV_DONTNEED; // Partial pages are not discarded (offset and length must be aligned)
      break;
    case 5:
      osAdvice = POSIX_FADV_NOREUSE; // Noop on Linux
      break;
  }

  int result = posix_fadvise(fd, (off_t) offset, (off_t) length, osAdvice);
  if (result != 0) {
    (*env)->ThrowNew(env, class_ioex, strerror(errno));
    return -1;
  }

  return 0;
}

 
/*
 * Class:     fr_micoq_unsafe_MappedMemory
 * Method:    madvise
 * Signature: (JJI)V
 */
JNIEXPORT jint JNICALL Java_fr_micoq_unsafe_MappedMemory_madvise(JNIEnv *env, jclass _ignore, jlong addr, jlong length, jint advice)
{
  size_t size = (size_t) length;

  int page = getpagesize();

  // round start down to start of page
  long long start = (long long) addr;
  start = start & (~(page-1));

  // round end up to start of page
  long long end = start + size;
  end = (end + page-1)&(~(page-1));
  size = (end-start);

  int osAdvice;
  switch(advice) {
    case 0:
      osAdvice = POSIX_MADV_NORMAL;
      break;
    case 1:
      osAdvice = POSIX_MADV_SEQUENTIAL; // Doesn't evict pages on Linux
      break;
    case 2:
      osAdvice = POSIX_MADV_RANDOM;
      break;
    case 3:
      osAdvice = POSIX_MADV_WILLNEED;
      break;
    case 4:
      osAdvice = POSIX_MADV_DONTNEED;
      break;
    case 5:
      return -1;
      break;
  }
  
  if (madvise((void *) start, size, osAdvice) != 0) {
    jclass class_ioex = (*env)->FindClass(env, "java/io/IOException");
    if (class_ioex == NULL) {
      return -1;
    }

    (*env)->ThrowNew(env, class_ioex, strerror(errno));
    return -1;
  }
  
  return 0;
}

/*
 * Class:     fr_micoq_unsafe_MappedMemory
 * Method:    unmap
 * Signature: (JJ)I;
 */
JNIEXPORT jint JNICALL Java_fr_micoq_unsafe_MappedMemory_munmap(JNIEnv *env, jclass _ignore, jlong address, jlong len)
{
  if (munmap((void *)address, (size_t)len) == -1) {
    jclass class_ioex = (*env)->FindClass(env, "java/io/IOException");
    if (class_ioex == NULL) {
      return -1;
    }
    (*env)->ThrowNew(env, class_ioex, strerror(errno));
    return -1; 
  }
  return 0;
}

/*
 * Class:     fr_micoq_unsafe_MappedMemory
 * Method:    closeDescriptor
 * Signature: (Ljava/io/FileDescriptor;)V
 */
JNIEXPORT jint JNICALL Java_fr_micoq_unsafe_MappedMemory_closeDescriptor(JNIEnv *env, jclass _ignore, jobject fileDescriptor)
{
  int fd;
  jfieldID field_fd;
  jclass class_fdesc;
  
  class_fdesc = (*env)->FindClass(env, "java/io/FileDescriptor");
  if (class_fdesc == NULL) {
    return -1;
  }

  field_fd = (*env)->GetFieldID(env, class_fdesc, "fd", "I");
  if (field_fd == NULL) {
    return -1;
  }

  fd = (*env)->GetIntField(env, fileDescriptor, field_fd);
  
  if(close(fd) == -1) {
    jclass class_ioex = (*env)->FindClass(env, "java/io/IOException");
    if (class_ioex == NULL) {
      return -1;
    }
    (*env)->ThrowNew(env, class_ioex, strerror(errno));
    return -1;
  }
  
  // Invalidate the fd
  (*env)->SetIntField(env, fileDescriptor, field_fd, -1);
  
  return 0;
}
 
/*
 * Class:     fr_micoq_unsafe_MappedMemory
 * Method:    mapFile
 * Signature: (Ljava/lang/String;)Lfr/micoq/unsafe/MappedMemory;
 */
JNIEXPORT jobject JNICALL Java_fr_micoq_unsafe_MappedMemory_mapFile(JNIEnv *env, jclass _ignore, jstring filename)
{
  int fd;
  char *fname;
  struct stat64 sb;
  jclass class_mapped_mem, class_ioex, class_fdesc;
  jfieldID field_addr, field_length, field_fd;
  jmethodID const_mapped_mem, const_fdesc;
  jobject object_mapped_mem, object_fdesc;
  void *mapAddress = 0;
  
  fname = (char *) (*env)->GetStringUTFChars(env,filename, NULL);
  fd = open(fname, O_RDONLY | O_NOATIME);
  
  class_ioex = (*env)->FindClass(env, "java/io/IOException");
  if (class_ioex == NULL) {
    return NULL;
  }
  
  class_fdesc = (*env)->FindClass(env,"java/io/FileDescriptor");
  if (class_fdesc == NULL) {
    return NULL;
  }
  
  if (fd < 0) {
    // open returned an error. Throw an IOException with the error string
    (*env)->ThrowNew(env, class_ioex, strerror(errno));
    return NULL;
  }
  
  if (fstat64(fd, &sb) == -1) {
    // fstat returned an error. Throw an IOException with the error string
    (*env)->ThrowNew(env, class_ioex, strerror(errno));
    return NULL;
  }
  
  if(sb.st_size == 0) {
    mapAddress = 0; // We cannot create a mapping on zero length file
  } else {  
    mapAddress = mmap64(0, sb.st_size, PROT_READ, MAP_SHARED, fd, 0);
  }
  
  if (mapAddress == MAP_FAILED) {
    jclass class_oom = (*env)->FindClass(env, "java/lang/OutOfMemoryError");
    if (class_oom == NULL) {
      close(fd);
      return NULL;
    }
    (*env)->ThrowNew(env, class_oom, strerror(errno));
    close(fd);
    return NULL;
  }
  
  class_mapped_mem = (*env)->FindClass(env, "fr/micoq/unsafe/MappedMemory");
  if (class_mapped_mem == NULL) {
    close(fd);
    return NULL;
  }
  
  // construct a new FileDescriptor
  const_fdesc = (*env)->GetMethodID(env, class_fdesc, "<init>", "(I)V");
  if (const_fdesc == NULL) {
    close(fd);
    return NULL;
  }
  object_fdesc = (*env)->NewObject(env, class_fdesc, const_fdesc, fd);
  
  // construct a new MappedMemory
  const_mapped_mem = (*env)->GetMethodID(env, class_mapped_mem, "<init>", "(JJLjava/io/FileDescriptor;)V");
  if (const_mapped_mem == NULL) {
    close(fd);
    return NULL;
  }
  object_mapped_mem = (*env)->NewObject(env, class_mapped_mem, const_mapped_mem, (long long) mapAddress, sb.st_size, object_fdesc);

  return object_mapped_mem;
}
