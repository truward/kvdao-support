package com.truward.kvdao.xodus.init;

import jetbrains.exodus.env.Environment;
import jetbrains.exodus.env.Environments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.FileSystemUtils;

import javax.annotation.PostConstruct;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Initializer for Xodus DB.
 */
public class XodusInitializer implements Closeable {
  private static final String TEMP_DIR_NAME = "xodus-test-db";

  private final Logger log = LoggerFactory.getLogger(getClass());

  private Environment environment;
  private Path tempDir;
  private boolean isTemp;
  private String dirName;

  public XodusInitializer() {
    setTemp(true);
    setDirName(TEMP_DIR_NAME);
  }

  @SuppressWarnings("SameParameterValue")
  public void setTemp(boolean temp) {
    isTemp = temp;
  }

  public void setDirName(String dirName) {
    this.dirName = dirName;
  }

  @PostConstruct
  public void initialize() throws IOException {
    final String dirPath;
    if (isTemp) {
      // Create temporary DB that will store all the files which will be deleted later
      tempDir = Files.createTempDirectory(dirName);
      dirPath = tempDir.toFile().getAbsolutePath();
    } else {
      dirPath = dirName;
    }

    if (!Files.exists(Paths.get(dirPath))) {
      throw new IOException("Can't open DB at dirPath=" + dirPath);
    }

    log.info("Trying to initialize environment at dirPath={}, isTemp={}", dirPath, isTemp);
    environment = Environments.newInstance(dirPath);
  }

  public void close() {
    if (environment != null) {
      try {
        environment.close();
      } finally {
        environment = null;
      }

      if (tempDir != null) {
        FileSystemUtils.deleteRecursively(tempDir.toFile());
        tempDir = null;
      }
    }

    environment = null;
    log.info("Environment closed at dirName={}", dirName);
  }

  public Environment getEnvironment() {
    if (environment == null) {
      throw new IllegalStateException("Environment has not been initialized yet");
    }
    return environment;
  }
}

