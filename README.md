# Native Unix Store plugin
This plugin adds the new store type `nativeunixfs` which uses native POSIX calls to speed up all I/O operations on indices (searches, merges, shards copies...) by reducing the filesystem cache consumption and by avoiding the unnecessary data loading from the storage devices.

The plugin is especially suitable for large indices/shards and works on SSDs too.

## How it works ?
The plugin has two parts which can be enabled separately.

### Mapped memory
The mapped memory enabled by default to read the segment files for search operations. It works like `mmapfs` by mapping the files into the virtual memory but with some enhancements: 
* the syscall `madvise()` is used on the mapped file with `MADV_RANDOM` to avoid the kernel to load more data than necessary. Without that, when a small part of a segment file is read by Elasticsearch (for example the terms list), large chunks are read (up to 2MB, see here: https://github.com/torvalds/linux/blob/master/mm/readahead.c#L236). This wastes the filesystem cache and uses a lot of IO.
* a file bigger than 1GB is mapped into a single map instead of multiples smalls maps of 1GB (due to the limitation of the ByteBuffer API).

These enhancements will decrease the response time of search operations.

### Direct I/O
Segment merges can consume a lot of filesystem cache. Merges on old (and large) segments can evict "hot" recent data. To avoid this, Linux can open the segment files on a special mode (`O_DIRECT`) to bypass the cache when reading or writing.

This part is a reimplementation of the preliminary code introduced by Lucene but still incomplete for the last Lucene versions (https://lucene.apache.org/core/7_4_0/misc/org/apache/lucene/store/NativeUnixDirectory.html).

The direct I/O is not enabled by default because it consume a lot of direct memory (for page-aligned direct buffers) and can lead to an out of memory error. Use with caution !

## Prerequisites
The plugin needs to be run on Linux x86_64 (Windows and other Un*x are not supported).

JVM compatilibty:
* OpenJDK 8
* OpenJDK 10
* OpenJDK 11

**Note**: Since the plugin uses internal API like `sun.misc.Unsafe`, the compatibility with other JVMs is not assured.

## Build

###  Prerequisites
You will need the **jdk 11** to run the embedded version of the Gradle wrapper and compile the java classes. **gcc** is required to build the native libraries.

On Debian, you can install the required packages with:
```
apt install openjdk-11-jdk gcc
```

### Build the plugin
Just clone/download the repository and execute this command at the root of the project directory:
```
JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 ./gradlew assemble
```
Note: `JAVA_HOME` must be set for the Elasticsearch Gradle plugin. 

The plugin zip file will be created in `native-unix-store/build/distributions`

## Installation
Use the standard `elasticsearch-plugin` command in your Elasticsearch directory:
```
bin/elasticsearch-plugin install file:///path/to/plugin.zip
```
Add this line in `config/jvm.options` 
```
-Djava.library.path=plugins/native-unix-store/native
```
**Note:** This option is required by the Elasticsearch JVM to locate the native libraries (*.so). The path is relative to the working directory of the JVM (by default it's the root of the Elasticsearch directory).

## Usage
### Index properties
The store type `nativeunixfs` come with some properties which can be configured at the index creation.

**Note**: All the properties can be dynamically changed but the index must be closed and reopened to take the changes into account.

#### `index.store.mmap.enabled`
Type: `boolean`
Default: `true`
Enable mapped memory (with `mmap()`) for all read operations that require caching (searches but not merges).
If disabled, these operations will use NIO (like with `niofs`)

#### `index.store.mmap.read_ahead` 
Type: `boolean`
Default: `false`
If enabled, this option tells the kernel to read ahead more data than requested to the filesystem cache (using the syscall `madvise()`) so the data will be read more sequentially on the storage device.
Enable this property helps to warm up the index quickly on static data but uses more RAM for the filesystem cache and more bandwidth on the storage device.
We recommend to disable this property for transient data (daily logs, time-series...).

#### `index.store.preload`
Type:`list`
Default: empty

Works like the same property with `mmapfs`: https://www.elastic.co/guide/en/elasticsearch/reference/master/_pre_loading_data_into_the_file_system_cache.html

#### `index.store.mmap.max_preload_size`
Type: `byte size`
Default: `0B` (unlimited)

When `index.store.preload` is used, files smaller than this size will never be preloaded.

#### `index.store.direct.read.enabled`
Type: `boolean`
Default: `false`

Enable direct I/O for all read operations that don't require caching (merges and recoveries).
If disabled, these operations will use NIO (like with `niofs`).

Enabling direct I/O helps to reduce the filesystem cache consumption during the merges or recoveries operations. However, the benefit of direct I/O depends on various parameters (the filesystem type, the kernel...).

**Note**: Direct I/O needs a lot of direct memory. To avoid `OutOfMemory` errors, you must set the maximum memory the JVM can use for the direct memory in `jvm.options`:
```
-XX:MaxDirectMemorySize=2g
```

#### `index.store.direct.write.enabled`
Type: `boolean`
Default: `false`

Enable direct I/O for all write operations.
If disabled, these operations will use NIO (like with `niofs`).

Enabling direct I/O helps to reduce the filesystem cache consumption during the merges or recoveries operations. However, the benefit of direct I/O depends on various parameters (the filesystem type, the kernel...).

**Note**: Direct I/O needs a lot of direct memory. To avoid `OutOfMemory` errors, you must set the maximum memory the JVM can use for the direct memory in `jvm.options`:
```
-XX:MaxDirectMemorySize=2g
```

#### `index.store.direct.min_merge_size`
Type: `byte size`
Default: `10MB`

The minimum merge size for which direct I/O will be used (only relevant when direct I/O are enabled).

#### `index.store.direct.write.buffer_size`
Type: `byte size`
Default: `128kB`
The buffer size for the direct write operations.

**Note:** Ensure you have enough allocable direct memory for the JVM since a buffer is allocated for each opened file.

#### `index.store.direct.read.buffer_size`
Type: `byte size`
Default: `128kB`
The buffer size for the direct read operations.

**Note:** Ensure you have enough allocable direct memory for the JVM since a buffer is allocated for each opened file.

### Examples
To create an index with a single 
```curl -XPUT http://localhost:9200/myindex --data-binary '{"index.store.type":"nativeunixfs","index.number_of_replicas":0,"index.number_of_shards":1}' -H 'Content-type: application/json'```

## Tests

The embedded tests uses the framework provided by Elasticsearch and Lucene.

### Unit tests
To run the embedded unit tests, use this command:
```
JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 ./gradlew testRandom
```

###  Integration tests
To run the embedded integration tests, use this command:
```
JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 ./gradlew integTest
```

## Issues

### Elasticsearch cannot start with an error `java.lang.UnsatisfiedLinkError`
The native libraries directory path is probably missing or incorrect. If you already specified the option `-Djava.library.path`, try to use the absolute path:
```
-Djava.library.path=/usr/share/elasticsearch/plugins/native-unix-store/native
```

### The index cannot be loaded with an error `IOException: Invalid argument`

If the index is stored on a `tmpfs` mount point, you must disable direct I/O since `tmpfs` rely on the filesystem cache and direct accesses bypass it.

Some other filesystem types could be incompatible with direct I/O.

### Elasticsearch crashes with an error `java.lang.OutOfMemoryError: Direct buffer memory`

Reduce the direct I/O memory buffers.

Reduce the number of segments/shards and reduce concurrent merges since each opened file in direct mode will use a exclusive buffer. 

Increase the allowed direct memory (`-XX:MaxDirectMemorySize=`).

Disable direct I/O for read/write or completely.

### Poor performance with direct I/O (high %wait CPU, device load...)

Increase the minimum merge size (`index.store.direct.min_merge_size`) to avoid using direct I/O for small and compound segments which are often rewritten.

If the index is stored on a `xfs` mount point, direct reads could evict memory pages from the filesystem cache while merging (https://github.com/HewlettPackard/LinuxKI/wiki/Poor-DirectIO-Reads). Try to store the index on another filesystem type.

Disable direct I/O for read operations.

## Contributing
Feel free to share, fork or reuse the code in your projects :)

You can also submit pull requests if you find a bug or an enhancement.

You can donate to my bitcoin (BTC) address: <a href="bitcoin:34mj5SYEMrbCKdQutir1XiQ5kWFAptoLUx">34mj5SYEMrbCKdQutir1XiQ5kWFAptoLUx</a>

