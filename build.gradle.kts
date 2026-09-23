plugins {
    java
    `maven-publish`
}

group = "org.example"
version = "1.0-SNAPSHOT"

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(25))
    }
}

repositories {
    mavenLocal()
    mavenCentral()
}

dependencies {
    implementation("org.jetbrains:annotations:16.0.2")
    implementation("org.apache.commons:commons-lang3:3.18.0") // Matched to your utility library

    // Standard PostgreSQL Driver
    implementation("org.postgresql:postgresql:42.7.2")
}
