package com.truward.kvdao.xodus.test;

import com.truward.kvdao.xodus.KeyUtil;
import com.truward.semantic.id.IdCodec;
import com.truward.semantic.id.SemanticIdCodec;
import jetbrains.exodus.ByteIterable;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link KeyUtil}.
 */
public class KeyUtilTest {
  private final IdCodec idCodec = SemanticIdCodec.forPrefixNames("entity1");

  @Test
  public void shouldDecodeAndEncodeSemanticIds() {
    final String id = idCodec.encodeBytes(new byte[] { 1, 2, 3 });
    final ByteIterable key = KeyUtil.semanticIdAsKey(idCodec, id);
    final String otherId = KeyUtil.keyAsSemanticId(idCodec, key);
    assertEquals(id, otherId);
  }
}
