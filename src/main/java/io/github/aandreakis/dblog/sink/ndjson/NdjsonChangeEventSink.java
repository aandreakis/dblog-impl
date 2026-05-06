package io.github.aandreakis.dblog.sink.ndjson;

import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.sink.api.ChangeEventSink;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Objects;

public final class NdjsonChangeEventSink implements ChangeEventSink {
  private final Writer writer;
  private final boolean closeWriter;
  private final NdjsonChangeEventEncoder encoder;

  public NdjsonChangeEventSink(Writer writer, boolean closeWriter) {
    this.writer = Objects.requireNonNull(writer, "writer");
    this.closeWriter = closeWriter;
    this.encoder = new NdjsonChangeEventEncoder();
  }

  public static NdjsonChangeEventSink forFile(Path path) {
    Objects.requireNonNull(path, "path");
    try {
      Path absolutePath = path.toAbsolutePath().normalize();
      Path parent = absolutePath.getParent();
      if (parent != null) {
        Files.createDirectories(parent);
      }
      // Append-only semantics so process restarts do not clobber prior output. This sink is
      // non-durable — it flushes per batch but does not fsync — so duplicates on replay and
      // event loss on power failure are expected tradeoffs. Operators needing durable output
      // should configure the JDBC apply sink instead.
      BufferedWriter writer =
          Files.newBufferedWriter(
              absolutePath,
              StandardCharsets.UTF_8,
              StandardOpenOption.CREATE,
              StandardOpenOption.APPEND,
              StandardOpenOption.WRITE);
      return new NdjsonChangeEventSink(writer, true);
    } catch (IOException ex) {
      throw new IllegalStateException("Failed to open NDJSON sink file: " + path, ex);
    }
  }

  public static NdjsonChangeEventSink stdout() {
    return new NdjsonChangeEventSink(
        new BufferedWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8)), false);
  }

  @Override
  public void appendEvents(List<ChangeEvent> events) {
    Objects.requireNonNull(events, "events");
    if (events.isEmpty()) {
      return;
    }
    try {
      for (ChangeEvent event : events) {
        writer.write(encoder.encode(event));
        writer.write('\n');
      }
      writer.flush();
    } catch (IOException ex) {
      throw new IllegalStateException("Failed to append NDJSON sink events", ex);
    }
  }

  @Override
  public void close() throws Exception {
    if (closeWriter) {
      writer.close();
    } else {
      writer.flush();
    }
  }
}
