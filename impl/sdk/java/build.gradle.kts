plugins {
    // Apply the java-library plugin for API and implementation separation.
    `java-library`

    // Apply Spotless for code formatting and linting.
    alias(libs.plugins.spotless)
}

group = "io.resonatehq"

version = "0.7.0"

description = "Distributed Async Await by Resonate HQ, Inc"

repositories {
    // Use Maven Central for resolving dependencies.
    mavenCentral()
}

dependencies {
    // JSON serialization / deserialization (mirrors Python's msgspec).
    implementation(libs.jackson.databind)

    // JUnit 5 test framework.
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
}

// Apply a specific Java toolchain to ease working on different environments.
java {
    toolchain { languageVersion = JavaLanguageVersion.of(21) }
}

tasks.named<Test>("test") { useJUnitPlatform() }

// Configure Spotless to format Java sources with Palantir Java Format.
spotless {
    java {
        palantirJavaFormat()
        removeUnusedImports()
        importOrder()
        trimTrailingWhitespace()
        endWithNewline()
    }
}
