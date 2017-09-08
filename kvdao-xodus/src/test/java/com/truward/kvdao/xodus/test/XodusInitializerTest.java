package com.truward.kvdao.xodus.test;

import com.truward.kvdao.xodus.init.XodusInitializer;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link com.truward.kvdao.xodus.init.XodusInitializer}
 */
public class XodusInitializerTest {

  @Test
  public void shouldDeleteTestEnvironment() throws IOException {
    final XodusInitializer initializer = new XodusInitializer();
    initializer.setTemp(true);
    initializer.initialize();

    assertNotNull("Environment is null", initializer.getEnvironment());
    final String location = initializer.getEnvironment().getLocation();
    final File file = new File(location);
    assertTrue("Environment dir should exist", file.exists());

    initializer.close();

    assertFalse("Environment dir should be cleaned up", file.exists());
  }
}
