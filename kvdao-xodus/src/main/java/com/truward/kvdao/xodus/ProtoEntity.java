package com.truward.kvdao.xodus;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ByteIterable;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.EmptyResultDataAccessException;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

/**
 * Protocol buffer entity de/serializer.
 *
 * @author Alexander Shabanov
 */
@ParametersAreNonnullByDefault
public final class ProtoEntity {
  private ProtoEntity() {} // hidden

  @SuppressWarnings("unchecked")
  public static <T extends Message> T entryToProto(@Nullable ByteIterable byteIterable, T defaultInstance) {
    if (byteIterable == null) {
      throw new EmptyResultDataAccessException(defaultInstance.getClass().getName(), 1);
    }

    final byte[] bytes = byteIterable.getBytesUnsafe();
    if (bytes == null) {
      throw new DataIntegrityViolationException("Can't get bytes that constitute an entity " + defaultInstance.getClass());
    }

    try {
      return (T) defaultInstance.getParserForType().parseFrom(bytes, 0, byteIterable.getLength());
    } catch (InvalidProtocolBufferException e) {
      throw new DataIntegrityViolationException("Can't parse entity " + defaultInstance.getClass(), e);
    }
  }

  public static <T extends Message> ByteIterable protoToEntry(T instance) {
    return new ArrayByteIterable(instance.toByteArray());
  }
}
