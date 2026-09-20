plugins {
    // Apply the java-library plugin for API and implementation separation.
    `java-library`

    // Apply the eclipse plugin so IDEs (VS Code / Eclipse JDT) put the custom
    // "examples" source set on the classpath instead of treating its files as
    // loose sources ("... is not on the classpath ... only syntax errors").
    eclipse

    // Apply Spotless for code formatting and linting.
    alias(libs.plugins.spotless)

    // Publish to Maven Central via the Central Portal. Handles sources/javadoc
    // jars, POM metadata, GPG signing, and upload in one plugin.
    alias(libs.plugins.maven.publish)
}

group = "io.resonatehq"

version = "0.1.1"

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

// Compile against a fixed Java 21 toolchain: this is the bytecode we ship.
java {
    toolchain { languageVersion = JavaLanguageVersion.of(21) }
}

// Surface javac lint warnings (serial, unchecked, deprecation, ...) and fail the build on them so
// CI catches regressions. The IDE-side equivalent lives in .settings/org.eclipse.jdt.core.prefs.
tasks.withType<JavaCompile>().configureEach {
    options.compilerArgs.addAll(listOf("-Xlint:all", "-Werror"))
}

tasks.named<Test>("test") {
    useJUnitPlatform()
    // Run the 21-built artifact on a configurable JVM to prove backward
    // compatibility. Override with -PtestJavaVersion=25 (CI matrices this).
    val testJavaVersion = (findProperty("testJavaVersion") as String? ?: "21").toInt()
    javaLauncher = javaToolchains.launcherFor { languageVersion = JavaLanguageVersion.of(testJavaVersion) }
}

// -- Examples ----------------------------------------------------------------
// The runnable example programs (the Java analogue of resonate-sdk-py/examples) live in their own
// source set so they compile against the library without polluting the published jar.
sourceSets {
    create("examples") {
        java.srcDir("src/examples/java")
        compileClasspath += sourceSets["main"].output
        runtimeClasspath += sourceSets["main"].output
    }
}

// The examples need the library's own dependencies (Jackson) on their compile/runtime classpaths.
configurations["examplesImplementation"].extendsFrom(configurations["implementation"])
configurations["examplesRuntimeOnly"].extendsFrom(configurations["runtimeOnly"])

// Compile (and thus lint) the examples as part of `check`, so API drift or warnings in them fail the
// build. They stay in their own source set, so they are still excluded from the published jar.
tasks.named("check") { dependsOn("compileExamplesJava") }

// Run an example, e.g.:
//   ./gradlew runExample -PmainClass=io.resonatehq.examples.fibonacci.Fibonacci -PexampleArgs="--mode rpc --n 10"
tasks.register<JavaExec>("runExample") {
    group = "examples"
    description = "Run an example program (-PmainClass=<fqcn> [-PexampleArgs=\"...\"])."
    classpath = sourceSets["examples"].runtimeClasspath
    mainClass.set(
        providers.gradleProperty("mainClass").orElse("io.resonatehq.examples.helloworld.HelloWorld"))
    providers.gradleProperty("exampleArgs").orNull?.let { args = it.split(" ").filter(String::isNotEmpty) }
}

// -- Publishing -------------------------------------------------------------
// Central Portal upload + signing. Credentials come from env vars in CI:
//   ORG_GRADLE_PROJECT_mavenCentralUsername
//   ORG_GRADLE_PROJECT_mavenCentralPassword
//   ORG_GRADLE_PROJECT_signingInMemoryKey
//   ORG_GRADLE_PROJECT_signingInMemoryKeyPassword
mavenPublishing {
    publishToMavenCentral(com.vanniktech.maven.publish.SonatypeHost.CENTRAL_PORTAL)
    signAllPublications()

    coordinates(group.toString(), "resonate-sdk-java", version.toString())

    pom {
        name.set("resonate-sdk-java")
        description.set(project.description)
        url.set("https://github.com/resonatehq/resonate-sdk-java")
        licenses {
            license {
                name.set("Apache License 2.0")
                url.set("https://www.apache.org/licenses/LICENSE-2.0")
            }
        }
        developers {
            developer {
                id.set("resonatehq")
                name.set("Resonate HQ, Inc")
                url.set("https://resonatehq.io")
            }
        }
        scm {
            url.set("https://github.com/resonatehq/resonate-sdk-java")
            connection.set("scm:git:git://github.com/resonatehq/resonate-sdk-java.git")
            developerConnection.set("scm:git:ssh://git@github.com/resonatehq/resonate-sdk-java.git")
        }
    }
}

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
