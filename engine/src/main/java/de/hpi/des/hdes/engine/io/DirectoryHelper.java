package de.hpi.des.hdes.engine.io;

import org.apache.commons.lang3.SystemUtils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DirectoryHelper {
    private static String packageRoot;
    private static String classPath;

    public static String getPackageRoot() {
        if (packageRoot == null) {
            packageRoot = System.getProperty("user.dir");
            if (!packageRoot.endsWith("/engine")) {
                packageRoot += "/engine";
            }
            packageRoot += "/src/main/java/";
            log.info("Determined the packageRoot path through user.Dir. Setting it to: {}", packageRoot);
        }

        return packageRoot;
    }

    public static String getTempDirectoryPath() {

        return getPackageRoot() + "de/hpi/des/hdes/engine/temp/";
    }

    public static void setPackageRoot(String packagePath) {
        if (packageRoot != null) {
            log.warn("Package root is already set to '{}'. Overwriting it with: {}", packagePath, packagePath);
        }
        packageRoot = packagePath;
    }

    public static String getClassPathWithTempPackage() {
        if (classPath == null) {
            String separotor = "";
            if (SystemUtils.IS_OS_UNIX) {
                separotor = ":";
            } else if (SystemUtils.IS_OS_WINDOWS) {
                separotor = ";";
            } else {
                log.error("Can't determine class path separator unknown OS: {}", System.getProperty("os.name"));
            }
            classPath = DirectoryHelper.getPackageRoot() + separotor + System.getProperty("java.class.path");
        }

        return classPath;
    }

}