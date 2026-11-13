plugins {
    id("com.android.application")
}

// The app version follows the engine release line it was built next to. CI passes
// -PappVersion=X.Y.Z from the release tag; local builds use the placeholder below.
val appVersion = (findProperty("appVersion") as String?) ?: "0.0.0"

fun versionCodeOf(version: String): Int {
    val parts = version.substringBefore('-').split('.').map { it.toIntOrNull() ?: 0 }
    val (major, minor, patch) = (parts + listOf(0, 0, 0)).take(3)
    return major * 1_000_000 + minor * 1_000 + patch
}

android {
    namespace = "io.github.jamezrin.outpace"
    compileSdk = 36

    defaultConfig {
        applicationId = "io.github.jamezrin.outpace"
        minSdk = 21
        targetSdk = 36
        versionName = appVersion
        versionCode = maxOf(1, versionCodeOf(appVersion))
    }

    signingConfigs {
        // Release signing comes from CI secrets; without them the release APK is unsigned.
        val storeFile = System.getenv("OUTPACE_ANDROID_KEYSTORE")
        if (storeFile != null) {
            create("release") {
                this.storeFile = file(storeFile)
                storePassword = System.getenv("OUTPACE_ANDROID_KEYSTORE_PASSWORD")
                keyAlias = System.getenv("OUTPACE_ANDROID_KEY_ALIAS")
                keyPassword = System.getenv("OUTPACE_ANDROID_KEY_PASSWORD")
            }
        }
    }

    buildTypes {
        release {
            isMinifyEnabled = true
            isShrinkResources = true
            proguardFiles(getDefaultProguardFile("proguard-android-optimize.txt"))
            signingConfigs.findByName("release")?.let { signingConfig = it }
        }
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }

    testOptions {
        unitTests.isReturnDefaultValues = false
    }

    lint {
        abortOnError = true
        checkReleaseBuilds = true
    }
}

dependencies {
    testImplementation("junit:junit:4.13.2")
    // android.jar's org.json is a stub on the JVM; tests use the real implementation.
    testImplementation("org.json:json:20250517")
}
