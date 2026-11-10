plugins {
    id("com.android.application")
    id("org.jetbrains.kotlin.android")
}

// Địa chỉ server gắn sẵn lúc build: biến môi trường MUSE_URL hoặc -PMUSE_URL=...
val museUrl: String = (System.getenv("MUSE_URL")?.takeIf { it.isNotBlank() }
    ?: providers.gradleProperty("MUSE_URL").orNull
    ?: "").trim().trimEnd('/')

android {
    namespace = "vn.muse.app"
    compileSdk = 34

    defaultConfig {
        applicationId = "vn.muse.app"
        minSdk = 26
        targetSdk = 34
        versionCode = 3
        versionName = "0.6.1"
        buildConfigField("String", "MUSE_URL", "\"${museUrl.replace("\"", "")}\"")
    }
    buildTypes {
        release {
            isMinifyEnabled = false
            signingConfig = signingConfigs.getByName("debug") // đổi sang keystore riêng khi phát hành
        }
    }
    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }
    kotlinOptions { jvmTarget = "17" }
    buildFeatures { buildConfig = true }
}

dependencies {
    implementation("androidx.core:core-ktx:1.13.1")
    implementation("androidx.activity:activity-ktx:1.9.1")
}
