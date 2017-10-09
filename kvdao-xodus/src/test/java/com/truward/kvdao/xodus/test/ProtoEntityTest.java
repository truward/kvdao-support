package com.truward.kvdao.xodus.test;

import com.google.protobuf.StringValue;
import com.truward.kvdao.xodus.ProtoEntity;
import jetbrains.exodus.ByteIterable;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public final class ProtoEntityTest {

  @Test
  public void shouldConvertToAndFromProtoEntry() {
    // Given:
    final StringValue value = StringValue.newBuilder().setValue("test").build();

    // When:
    final ByteIterable byteIterable = ProtoEntity.protoToEntry(value);
    final StringValue other = ProtoEntity.entryToProto(byteIterable, StringValue.getDefaultInstance());

    // Then:
    assertEquals(value, other);
  }
}
