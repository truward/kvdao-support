package com.truward.kvdao.xodus.test;

import com.truward.kvdao.xodus.XodusMetadata;
import com.truward.kvdao.xodus.init.XodusInitializer;
import com.truward.kvdao.xodus.metadata.MetadataDao;
import jetbrains.exodus.env.Environment;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.*;

/**
 * Tests for {@link com.truward.kvdao.xodus.init.XodusInitializer}
 */
public class XodusInitializerTest {

  @Test
  public void shouldCreateAndDeleteTestEnvironment() throws IOException {
    final XodusInitializer initializer = new XodusInitializer();
    initializer.setTemp(true);
    initializer.initialize();

    final Environment env = initializer.getEnvironment();
    assertNotNull("Environment is null", env);
    final String location = env.getLocation();
    final File file = new File(location);
    assertTrue("Environment dir should exist", file.exists());

    // test version on the fresh DB roll out (should be missing)
    final String firstVersion = "1.0";
    try {
      XodusMetadata.verifyVersion(env, firstVersion);
      fail("Version should be missing in the fresh DB");
    } catch (IllegalStateException ignored) {}

    // set first version
    final MetadataDao metadataDao = new MetadataDao(env);
    env.executeInExclusiveTransaction(tx -> metadataDao.create(tx, firstVersion));

    // verify version again
    XodusMetadata.verifyVersion(env, firstVersion);


    // update version
    final String secondVersion = "2.0";

    try {
      env.executeInExclusiveTransaction(tx -> metadataDao.create(tx, secondVersion));
      fail("Second creation of that version should fail");
    } catch (IllegalStateException ignored) {}

    env.executeInExclusiveTransaction(tx -> metadataDao.update(tx, firstVersion, secondVersion));

    // verify version again
    XodusMetadata.verifyVersion(env, secondVersion);

    initializer.close();

    assertFalse("Environment dir should be cleaned up", file.exists());
  }
}
