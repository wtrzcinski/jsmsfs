rootProject.name = "jsmsfs"

dependencyResolutionManagement {
    versionCatalogs {
        create("libs") {
            val jUnitVersion = "6.0.1"
            library("junit-jupiter-engine", "org.junit.jupiter", "junit-jupiter-engine").version(jUnitVersion)
            library("junit-jupiter-params", "org.junit.jupiter", "junit-jupiter-params").version(jUnitVersion)
            library("junit-platform-suite", "org.junit.platform", "junit-platform-suite").version(jUnitVersion)
            library("junit-platform-launcher", "org.junit.platform", "junit-platform-launcher").version(jUnitVersion)
        }
    }
}