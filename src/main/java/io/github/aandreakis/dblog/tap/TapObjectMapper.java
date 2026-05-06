package io.github.aandreakis.dblog.tap;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.github.aandreakis.dblog.tap.generated.StreamResumed;
import io.github.aandreakis.dblog.tap.generated.StreamStandby;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Builds the shared Jackson {@link ObjectMapper} used for tap serialisation.
 *
 * <p>Two tap-specific customisations layered on a default mapper:
 *
 * <ul>
 *   <li>A custom {@link Instant} serialiser emitting ISO-8601 with exactly six fractional-second
 *       digits. The schema calls this "microsecond resolution" and earlier hand-rolled code used
 *       the same fixed-width format; Jackson's default {@code InstantSerializer} trims trailing
 *       zeroes and would break wire parity.
 *   <li>Mixins on {@link StreamStandby} and {@link StreamResumed} that force {@code seq} to emit
 *       as an explicit {@code null} rather than being omitted under the generated class-level
 *       {@code @JsonInclude(NON_NULL)}. Matches {@code "seq": { "type": "null" }} in the two
 *       out-of-band schemas.
 * </ul>
 */
final class TapObjectMapper {
  private static final DateTimeFormatter TIMESTAMP_FORMATTER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'").withZone(ZoneOffset.UTC);

  private TapObjectMapper() {}

  static ObjectMapper create() {
    ObjectMapper mapper = new ObjectMapper();
    SimpleModule module = new SimpleModule();
    module.addSerializer(Instant.class, new MicrosecondInstantSerializer());
    mapper.registerModule(module);
    mapper.addMixIn(StreamStandby.class, SeqAlwaysMixin.class);
    mapper.addMixIn(StreamResumed.class, SeqAlwaysMixin.class);
    return mapper;
  }

  private static final class MicrosecondInstantSerializer extends JsonSerializer<Instant> {
    @Override
    public void serialize(Instant value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      gen.writeString(TIMESTAMP_FORMATTER.format(value));
    }
  }

  private abstract static class SeqAlwaysMixin {
    @JsonProperty("seq")
    @JsonInclude(JsonInclude.Include.ALWAYS)
    Object seq;
  }
}
