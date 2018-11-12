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

// WARNING: only for x86_64 arch !
 
#ifndef _GNU_SOURCE
#define _GNU_SOURCE    // for O_DIRECT
#endif

#include <jni.h>
#include <fcntl.h>     // posix_fadvise, constants for open
#include <string.h>    // strerror
#include <errno.h>     // errno
#include <unistd.h>    // getpagesize
#include <sys/types.h> // constants for open
#include <sys/stat.h>  // constants for open

/*
 * Class:     fr_micq_unsafe_DirectIO
 * Method:    openDirect
 * Signature: (Ljava/lang/String;Z)Ljava/io/FileDescriptor;
 */
JNIEXPORT jobject JNICALL Java_fr_micq_unsafe_DirectIO_openDirect(JNIEnv *env, jclass _ignore, jstring filename, jboolean readOnly)
{
  jfieldID field_fd;
  jmethodID const_fdesc;
  jclass class_fdesc, class_ioex;
  jobject ret;
  int fd;
  char *fname;

  class_ioex = (*env)->FindClass(env, "java/io/IOException");
  if (class_ioex == NULL) {
    return NULL;
  }
  class_fdesc = (*env)->FindClass(env, "java/io/FileDescriptor");
  if (class_fdesc == NULL) {
    return NULL;
  }

  fname = (char *) (*env)->GetStringUTFChars(env, filename, NULL);

  if (readOnly) {
    fd = open(fname, O_RDONLY | O_DIRECT | O_NOATIME);
  } else {
    fd = open(fname, O_RDWR | O_CREAT | O_DIRECT | O_NOATIME, 0666);
  }

  (*env)->ReleaseStringUTFChars(env,filename, fname);

  if (fd < 0) {
    // open returned an error. Throw an IOException with the error string
    (*env)->ThrowNew(env, class_ioex, strerror(errno));
    return NULL;
  }

  // construct a new FileDescriptor
  const_fdesc = (*env)->GetMethodID(env, class_fdesc, "<init>", "()V");
  if (const_fdesc == NULL) {
    return NULL;
  }
  ret = (*env)->NewObject(env, class_fdesc, const_fdesc);

  // poke the "fd" field with the file descriptor
  field_fd = (*env)->GetFieldID(env, class_fdesc, "fd", "I");
  if (field_fd == NULL) {
    return NULL;
  }
  (*env)->SetIntField(env, ret, field_fd, fd);

  // and return it
  return ret;
}
