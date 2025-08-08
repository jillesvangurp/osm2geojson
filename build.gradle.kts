plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.serialization)
}

repositories {
    mavenCentral()
}

dependencies {
    implementation(libs.commons.io)
    implementation(libs.commons.compress)
    implementation(libs.logback.classic)
    implementation(libs.kotlinx.serialization.json)

    testImplementation(libs.testng) {
        exclude(group = "junit", module = "junit")
    }
    testImplementation(libs.hamcrest)
    testImplementation(libs.kotest.assertions.core)
}

tasks.test {
    useTestNG()
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}
