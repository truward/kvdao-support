package com.truward.kvdao.xodus;

import com.truward.kvdao.xodus.metadata.MetadataDao;
import jetbrains.exodus.env.Environment;

import javax.annotation.ParametersAreNonnullByDefault;
import java.util.Optional;

/**
 * Metadata, associated with the catalog database
 */
@ParametersAreNonnullByDefault
public final class XodusMetadata {
  private XodusMetadata() {}

  /**
   * Store name, that holds database with meta information, such as current version of the database.
   */
  public static final String METADATA_STORE_NAME = "meta";

  /**
   * Special structure, holding version entry.
   */
  public static final String VERSION_KEY = "version";

  /**
   * Check, that expected version matches what is stored in the database.
   *
   * @param environment Xodus environment
   * @param expectedVersion Version to check
   */
  public static void verifyVersion(Environment environment, String expectedVersion) {
    final MetadataDao metadataDao = new MetadataDao(environment);
    final Optional<String> actualVersionOpt = environment.computeInReadonlyTransaction(metadataDao::getVersion);
    if (!actualVersionOpt.isPresent()) {
      throw new IllegalStateException("Missing version in metadata table");
    }

    final String actualVersion = actualVersionOpt.get();
    if (!actualVersion.equals(expectedVersion)) {
      throw new IllegalStateException("Database version mismatch; found=" + actualVersion +
          ", expected=" + expectedVersion);
    }
  }
}
