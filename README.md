Java simple MemorySegment FileSystem
------------------------------------

[![license](https://img.shields.io/badge/license-Apache%20License%202.0-blue.svg?style=flat)](https://www.apache.org/licenses/LICENSE-2.0)

Java simple [File System](https://docs.oracle.com/en/java/javase/25/docs/api/java.base/java/nio/file/package-summary.html) 
based on [Memory Segment](https://docs.oracle.com/en/java/javase/25/docs/api/java.base/java/lang/foreign/package-summary.html)

Usage
-----

```kotlin
val givenEnv = mapOf("capacity" to "1MB", "blockSize" to "1KB")
val givenFileSystem = FileSystems.newFileSystem(URI.create("jsmsfs:///"), givenEnv)
val givenDirectoryPath = givenFileSystem.getPath("directory")
val givenFilePath = givenDirectoryPath.resolve("file.txt")
val givenFileContent = newAlphanumericString(minLength = 1024, maxLength = 1024 * 4)

Files.createDirectory(givenDirectoryPath)
Files.writeString(givenFilePath, givenFileContent, Charsets.UTF_16)

assertThat(Files.exists(givenDirectoryPath)).isTrue()
assertThat(Files.exists(givenFilePath)).isTrue()
assertThat(Files.readString(givenFilePath, Charsets.UTF_16)).isEqualTo(givenFileContent)
```

Build
-----

```
./gradlew clean check
```

License
-------

```
Copyright 2025 Wojciech Trzciński

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```
