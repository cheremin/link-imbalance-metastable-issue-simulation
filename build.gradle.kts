import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.5.10"
    application

//    id("org.openjfx.javafxplugin") version "0.0.10"
}

group = "me.ruslan"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("com.github.holgerbrandl:kalasim:0.7.99")
    implementation("com.github.holgerbrandl:kravis:0.8.5")

    implementation("com.github.holgerbrandl:krangl:0.17.2")

    implementation("org.jetbrains.lets-plot:lets-plot-jfx:2.3.0")
    implementation("org.jetbrains.lets-plot:lets-plot-kotlin-jvm:3.2.0")


    testImplementation(kotlin("test"))
    testImplementation("org.hamcrest:hamcrest:2.2")
    testImplementation("org.junit.jupiter:junit-jupiter-params:5.0.0")
//    <version>5.0.0</version>
}

tasks.test {
    useJUnitPlatform()
    maxHeapSize = "8g"
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "11"
}

application {
    mainClass.set("MainKt")
}

//javafx {
//    version = "17.0.1"
//    modules("javafx.controls", "javafx.swing", "javafx.fxml")
//}