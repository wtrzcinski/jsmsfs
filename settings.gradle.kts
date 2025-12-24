rootProject.name = "jsmsfs"

include("lib:core", "lib:test")

dependencyResolutionManagement {
    versionCatalogs {
        create("libs") {
            val jUnitVersion = "6.0.1"
            library("junit-jupiter-engine", "org.junit.jupiter", "junit-jupiter-engine").version(jUnitVersion)
            library("junit-jupiter-params", "org.junit.jupiter", "junit-jupiter-params").version(jUnitVersion)
            library("junit-platform-suite", "org.junit.platform", "junit-platform-suite").version(jUnitVersion)
            library("junit-platform-launcher", "org.junit.platform", "junit-platform-launcher").version(jUnitVersion)

            val slf4jVersion = "2.0.17"
            library("slf4j-api", "org.slf4j", "slf4j-api").version(slf4jVersion)
            library("slf4j-simple", "org.slf4j", "slf4j-simple").version(slf4jVersion)

            val assertjVersion = "3.27.6"
            library("assertj-core", "org.assertj", "assertj-core").version(assertjVersion)
        }
    }
}