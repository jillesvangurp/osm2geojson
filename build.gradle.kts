plugins {
    kotlin("jvm") version "2.2.0"
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("commons-io:commons-io:2.4")
    implementation("com.google.guava:guava:17.0")
    implementation("commons-lang:commons-lang:2.6")
    implementation("com.jillesvangurp:iterables-support:1.8")
    implementation("org.apache.commons:commons-compress:1.8.1")
    implementation("com.jillesvangurp:jsonj:1.44")
    implementation("ch.qos.logback:logback-classic:1.1.2")

    testImplementation("org.testng:testng:6.8.8") {
        exclude(group = "junit", module = "junit")
    }
    testImplementation("org.hamcrest:hamcrest-all:1.3")
    testImplementation("org.mockito:mockito-all:1.9.5") {
        exclude(group = "org.hamcrest", module = "hamcrest-core")
    }
    testImplementation("io.kotest:kotest-assertions-core:5.7.2")
}

tasks.test {
    useTestNG()
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}
