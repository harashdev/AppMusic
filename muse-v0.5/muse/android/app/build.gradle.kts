plugins {
    id("com.android.application")
    id("org.jetbrains.kotlin.android")
}

// Địa chỉ server gắn sẵn lúc build: biến môi trường MUSE_URL hoặc -PMUSE_URL=...
val museUrl: String = (System.getenv("MUSE_URL")?.takeIf { it.isNotBlank() }
    ?: providers.gradleProperty("MUSE_URL").orNull
    ?: "https://appmusic-ur3z.onrender.com").trim().trimEnd('/')

// Discord Social SDK (tùy chọn): CI giải nén SDK vào các thư mục này. Thiếu thì app vẫn build, chỉ không có tính năng.
val sdkDir: String = (System.getenv("DISCORD_SDK_DIR") ?: "").trim()
val aarDir: String = (System.getenv("DISCORD_AAR_DIR") ?: "").trim()
val hasSdk = sdkDir.isNotEmpty() && aarDir.isNotEmpty() && file("$sdkDir/include/discordpp.h").exists() && file("libs/discord_partner_sdk.aar").exists()

android {
    namespace = "vn.muse.app"
    compileSdk = 34
    if (hasSdk) ndkVersion = "26.3.11579264"

    defaultConfig {
        applicationId = "vn.muse.app"
        minSdk = 26
        targetSdk = 34
        versionCode = 6
        versionName = "0.8.0"
        buildConfigField("String", "MUSE_URL", "\"${museUrl.replace("\"", "")}\"")
        if (hasSdk) {
            ndk { abiFilters += listOf("arm64-v8a", "armeabi-v7a") }
            externalNativeBuild {
                cmake {
                    arguments += listOf(
                        "-DANDROID_STL=c++_static",
                        "-DDISCORD_SDK_DIR=$sdkDir",
                        "-DDISCORD_AAR_DIR=$aarDir",
                        "-DMUSE_NATIVE_DIR=${rootDir.parentFile.resolve("native").absolutePath}"
                    )
                }
            }
        }
    }
    if (hasSdk) {
        externalNativeBuild { cmake { path = file("src/main/cpp/CMakeLists.txt") } }
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
    // Giao diện web nhúng sẵn trong APK (assets/shell.html, copy từ server/static/index.html lúc build).
    sourceSets { getByName("main") { assets.srcDir(layout.buildDirectory.dir("generated/shell")) } }
    // libmuse_audio.so là chương trình chạy được (không phải thư viện JNI): phải giải nén ra thư mục native thì mới chạy được.
    packaging {
        jniLibs { useLegacyPackaging = true }
        // Các thư viện của NewPipeExtractor (rhino, jsoup, protobuf...) mang theo file giấy phép trùng tên trong META-INF.
        resources { excludes += setOf("META-INF/DEPENDENCIES", "META-INF/LICENSE*", "META-INF/NOTICE*", "META-INF/INDEX.LIST") }
    }
}

val copyShell = tasks.register<Copy>("copyShell") {
    from(rootDir.parentFile.resolve("server/static/index.html"))
    rename { "shell.html" }
    into(layout.buildDirectory.dir("generated/shell"))
}
tasks.configureEach { if (name.startsWith("merge") && name.endsWith("Assets")) dependsOn(copyShell) }

dependencies {
    implementation("androidx.core:core-ktx:1.13.1")
    implementation("androidx.activity:activity-ktx:1.9.1")
    // Lấy link nhạc YouTube giống NewPipe/SimpMusic: giải mã chữ ký + tham số n, đổi client khi YouTube đổi. (GPL-3.0, cùng giấy phép repo)
    implementation("com.github.TeamNewPipe:NewPipeExtractor:v0.26.5")
    if (hasSdk) implementation(files("libs/discord_partner_sdk.aar"))
}
