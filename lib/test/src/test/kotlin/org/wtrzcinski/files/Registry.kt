package org.wtrzcinski.files

import org.assertj.core.api.Assertions
import org.wtrzcinski.files.memory.node.DirectoryNode
import org.wtrzcinski.files.memory.path.AbstractFilePath
import org.wtrzcinski.files.memory.path.HardFilePath
import org.wtrzcinski.files.memory.util.Require
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.random.Random

class Registry {
    private val regular = ConcurrentHashMap<HardFilePath, String>()
    private val links = ConcurrentHashMap<HardFilePath, HardFilePath>()
    private val directories = CopyOnWriteArrayList<HardFilePath>()

    private fun count(): Int {
        return regular.size + directories.size + links.size
    }

    fun addDirectory(root: HardFilePath) {
        directories.add(root)
    }

    fun checkRandomFile() {
        val entry = regular.entries.randomOrNull()
        if (entry != null) {
            val actual = Files.readString(entry.key)
            Assertions.assertThat(actual).isEqualTo(entry.value)
        }
    }

    fun checkRandomLink() {
        val entry = links.entries.randomOrNull()
        if (entry != null) {
            val link = entry.key
            val file = entry.value
            if (Files.exists(file)) {
                val fileContent = regular[file]
                val actual = Files.readString(file)
                Assertions.assertThat(actual).isEqualTo(fileContent)
            } else {
//                val actual = Files.readString(file)
//                Assertions.assertThat(actual).isEqualTo("")
            }
        }
    }

    fun createRandom() {
        val parent = directories.random()
        val nextInt = Random.nextInt(from = 0, until = 4)
        if (nextInt == 0) {
            createRandomDirectory(parent = parent)
        } else if (nextInt == 1) {
            createRandomLink(parent = parent)
        } else {
            createRandomRegularFile(parent = parent)
        }
    }

    fun deleteRandom() {
        val randomParent: HardFilePath = directories.random()
        deleteRandom(randomParent)
    }

    tailrec fun deleteRandom(directory: HardFilePath): Boolean {
        val node = directory.node
        require(node is DirectoryNode)

        val children = Files.list(directory).toList()
        if (children.isEmpty()) {
            Files.delete(directory)
            require(directories.remove(directory))
            return true
        } else {
            val file = children.random()
            require(file is HardFilePath)

            if (Files.isDirectory(file)) {
                return deleteRandom(directory = file)
            } else if (Files.isRegularFile(file)) {
                Files.delete(file)
                requireNotNull(regular.remove(file))
                return true
            } else if (Files.isSymbolicLink(file)) {
                Files.delete(file)
                requireNotNull(links.remove(file))
                return true
            }
        }

        Require.unreachable()
    }

    private fun createRandomRegularFile(parent: Path) {
        val childName = Fixtures.newAlphanumericString(
            lengthFrom = MonkeyTest.minStringSize,
            lengthUntil = MonkeyTest.maxStringSize
        )
        val childContent = Fixtures.newAlphanumericString(
            lengthFrom = MonkeyTest.minStringSize,
            lengthUntil = MonkeyTest.maxStringSize
        )
        val child: Path = parent.resolve(childName)
        val createFile = Files.createFile(child)
        Files.writeString(createFile, childContent, StandardOpenOption.WRITE)
        Assertions.assertThat(Files.exists(createFile)).isTrue()
        require(createFile is AbstractFilePath)
        regular[createFile.toRealPath()] = childContent
    }

    private fun createRandomDirectory(parent: Path) {
        val childName = Fixtures.newAlphanumericString(
            lengthFrom = MonkeyTest.minStringSize,
            lengthUntil = MonkeyTest.maxStringSize
        )
        val child = parent.resolve(childName)
        val createDirectory = Files.createDirectory(child)
        Assertions.assertThat(Files.exists(createDirectory)).isTrue()
        require(createDirectory is AbstractFilePath)
        directories.add(createDirectory.toRealPath())
    }

    private fun createRandomLink(parent: Path) {
        val childName = Fixtures.newAlphanumericString(
            lengthFrom = MonkeyTest.minStringSize,
            lengthUntil = MonkeyTest.maxStringSize
        )
        val child = parent.resolve(childName)
        val randomFile = regular.keys.randomOrNull()
        if (randomFile != null) {
            val createSymbolicLink = Files.createSymbolicLink(child, randomFile)
            Assertions.assertThat(Files.exists(createSymbolicLink)).isTrue()
            require(createSymbolicLink is AbstractFilePath)
            links[createSymbolicLink.toRealPath()] = randomFile
        }
    }
}