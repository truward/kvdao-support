package com.truward.kvdao.xodus.metadata;

import com.google.protobuf.StringValue;
import com.truward.kvdao.xodus.ProtoEntity;
import com.truward.kvdao.xodus.XodusMetadata;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.env.Environment;
import jetbrains.exodus.env.Store;
import jetbrains.exodus.env.StoreConfig;
import jetbrains.exodus.env.Transaction;

import java.util.Objects;
import java.util.Optional;

import static com.truward.kvdao.xodus.ProtoEntity.protoToEntry;

/**
 * Base class for working with metadata.
 */
public class MetadataDao {
  protected final Environment environment;
  protected final Store metaStore;

  public MetadataDao(Environment environment) {
    this.environment = Objects.requireNonNull(environment);
    this.metaStore = environment.computeInTransaction(tx -> environment.openStore(
        XodusMetadata.METADATA_STORE_NAME,
        StoreConfig.WITHOUT_DUPLICATES,
        tx));
  }

  public Optional<String> getVersion(Transaction tx) {
    final ByteIterable rawVersion = this.metaStore.get(tx, getVersionKey());
    if (rawVersion == null) {
      return Optional.empty();
    }

    return Optional.of(ProtoEntity.entryToProto(rawVersion, StringValue.getDefaultInstance()).getValue());
  }

  public void create(Transaction tx, String initialVersion) {
    final Optional<String> existingVersion = getVersion(tx);
    if (existingVersion.isPresent()) {
      throw new IllegalStateException("Version exists, cannot create a new one");
    }

    this.metaStore.put(tx, getVersionKey(), protoToEntry(StringValue.newBuilder().setValue(initialVersion).build()));
  }

  public void update(Transaction tx, String fromVersion, String toVersion) {
    final Optional<String> existingVersion = getVersion(tx);
    if (!existingVersion.isPresent()) {
      throw new IllegalStateException("Missing version in the database, expected version=" + fromVersion);
    }

    if (!existingVersion.get().equals(fromVersion)) {
      throw new IllegalStateException("Version mismatch, expected version=" + fromVersion +
          ", actual version=" + existingVersion.get());
    }

    this.metaStore.put(tx, getVersionKey(), protoToEntry(StringValue.newBuilder().setValue(toVersion).build()));
  }

  //
  // Protected
  //

  protected static ByteIterable getVersionKey() {
    return ProtoEntity.protoToEntry(StringValue.newBuilder().setValue(XodusMetadata.VERSION_KEY).build());
  }
}
