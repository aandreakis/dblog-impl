
package io.github.aandreakis.dblog.tap.generated;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonValue;


/**
 * WatermarkWritten
 * <p>
 * DBLog wrote a LOW or HIGH watermark row on dblog_meta.watermarks.
 * 
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
    "v",
    "seq",
    "ts",
    "run_id",
    "source_id",
    "kind",
    "level",
    "token",
    "chunk_id"
})
public class WatermarkWritten {

    /**
     * Tap schema version; always 1.
     * (Required)
     * 
     */
    @JsonProperty("v")
    @JsonPropertyDescription("Tap schema version; always 1.")
    private Long v;
    /**
     * Monotonic per-run sequence number.
     * 
     */
    @JsonProperty("seq")
    @JsonPropertyDescription("Monotonic per-run sequence number.")
    private Long seq;
    /**
     * ISO-8601 UTC timestamp with microsecond resolution.
     * (Required)
     * 
     */
    @JsonProperty("ts")
    @JsonPropertyDescription("ISO-8601 UTC timestamp with microsecond resolution.")
    private Instant ts;
    /**
     * DBLog runtime run identifier.
     * (Required)
     * 
     */
    @JsonProperty("run_id")
    @JsonPropertyDescription("DBLog runtime run identifier.")
    private String runId;
    /**
     * Configured dblog.source.id.
     * (Required)
     * 
     */
    @JsonProperty("source_id")
    @JsonPropertyDescription("Configured dblog.source.id.")
    private String sourceId;
    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("kind")
    private WatermarkWritten.Kind kind;
    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("level")
    private WatermarkWritten.Level level;
    /**
     * UUID written on the watermark row.
     * (Required)
     * 
     */
    @JsonProperty("token")
    @JsonPropertyDescription("UUID written on the watermark row.")
    private String token;
    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("chunk_id")
    private Long chunkId;

    /**
     * No args constructor for use in serialization
     * 
     */
    public WatermarkWritten() {
    }

    /**
     * 
     * @param sourceId
     *     Configured dblog.source.id.
     * @param v
     *     Tap schema version; always 1.
     * @param runId
     *     DBLog runtime run identifier.
     * @param ts
     *     ISO-8601 UTC timestamp with microsecond resolution.
     * @param token
     *     UUID written on the watermark row.
     */
    public WatermarkWritten(Long v, Instant ts, String runId, String sourceId, WatermarkWritten.Kind kind, WatermarkWritten.Level level, String token, Long chunkId) {
        super();
        this.v = v;
        this.ts = ts;
        this.runId = runId;
        this.sourceId = sourceId;
        this.kind = kind;
        this.level = level;
        this.token = token;
        this.chunkId = chunkId;
    }

    public static WatermarkWritten.WatermarkWrittenBuilderBase builder() {
        return new WatermarkWritten.WatermarkWrittenBuilder();
    }

    /**
     * Tap schema version; always 1.
     * (Required)
     * 
     */
    @JsonProperty("v")
    public Long getV() {
        return v;
    }

    /**
     * Tap schema version; always 1.
     * (Required)
     * 
     */
    @JsonProperty("v")
    public void setV(Long v) {
        this.v = v;
    }

    /**
     * Monotonic per-run sequence number.
     * 
     */
    @JsonProperty("seq")
    public Long getSeq() {
        return seq;
    }

    /**
     * Monotonic per-run sequence number.
     * 
     */
    @JsonProperty("seq")
    public void setSeq(Long seq) {
        this.seq = seq;
    }

    /**
     * ISO-8601 UTC timestamp with microsecond resolution.
     * (Required)
     * 
     */
    @JsonProperty("ts")
    public Instant getTs() {
        return ts;
    }

    /**
     * ISO-8601 UTC timestamp with microsecond resolution.
     * (Required)
     * 
     */
    @JsonProperty("ts")
    public void setTs(Instant ts) {
        this.ts = ts;
    }

    /**
     * DBLog runtime run identifier.
     * (Required)
     * 
     */
    @JsonProperty("run_id")
    public String getRunId() {
        return runId;
    }

    /**
     * DBLog runtime run identifier.
     * (Required)
     * 
     */
    @JsonProperty("run_id")
    public void setRunId(String runId) {
        this.runId = runId;
    }

    /**
     * Configured dblog.source.id.
     * (Required)
     * 
     */
    @JsonProperty("source_id")
    public String getSourceId() {
        return sourceId;
    }

    /**
     * Configured dblog.source.id.
     * (Required)
     * 
     */
    @JsonProperty("source_id")
    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("kind")
    public WatermarkWritten.Kind getKind() {
        return kind;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("kind")
    public void setKind(WatermarkWritten.Kind kind) {
        this.kind = kind;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("level")
    public WatermarkWritten.Level getLevel() {
        return level;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("level")
    public void setLevel(WatermarkWritten.Level level) {
        this.level = level;
    }

    /**
     * UUID written on the watermark row.
     * (Required)
     * 
     */
    @JsonProperty("token")
    public String getToken() {
        return token;
    }

    /**
     * UUID written on the watermark row.
     * (Required)
     * 
     */
    @JsonProperty("token")
    public void setToken(String token) {
        this.token = token;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("chunk_id")
    public Long getChunkId() {
        return chunkId;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("chunk_id")
    public void setChunkId(Long chunkId) {
        this.chunkId = chunkId;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(WatermarkWritten.class.getName()).append('@').append(Integer.toHexString(System.identityHashCode(this))).append('[');
        sb.append("v");
        sb.append('=');
        sb.append(((this.v == null)?"<null>":this.v));
        sb.append(',');
        sb.append("seq");
        sb.append('=');
        sb.append(((this.seq == null)?"<null>":this.seq));
        sb.append(',');
        sb.append("ts");
        sb.append('=');
        sb.append(((this.ts == null)?"<null>":this.ts));
        sb.append(',');
        sb.append("runId");
        sb.append('=');
        sb.append(((this.runId == null)?"<null>":this.runId));
        sb.append(',');
        sb.append("sourceId");
        sb.append('=');
        sb.append(((this.sourceId == null)?"<null>":this.sourceId));
        sb.append(',');
        sb.append("kind");
        sb.append('=');
        sb.append(((this.kind == null)?"<null>":this.kind));
        sb.append(',');
        sb.append("level");
        sb.append('=');
        sb.append(((this.level == null)?"<null>":this.level));
        sb.append(',');
        sb.append("token");
        sb.append('=');
        sb.append(((this.token == null)?"<null>":this.token));
        sb.append(',');
        sb.append("chunkId");
        sb.append('=');
        sb.append(((this.chunkId == null)?"<null>":this.chunkId));
        sb.append(',');
        if (sb.charAt((sb.length()- 1)) == ',') {
            sb.setCharAt((sb.length()- 1), ']');
        } else {
            sb.append(']');
        }
        return sb.toString();
    }

    @Override
    public int hashCode() {
        int result = 1;
        result = ((result* 31)+((this.sourceId == null)? 0 :this.sourceId.hashCode()));
        result = ((result* 31)+((this.level == null)? 0 :this.level.hashCode()));
        result = ((result* 31)+((this.v == null)? 0 :this.v.hashCode()));
        result = ((result* 31)+((this.kind == null)? 0 :this.kind.hashCode()));
        result = ((result* 31)+((this.runId == null)? 0 :this.runId.hashCode()));
        result = ((result* 31)+((this.chunkId == null)? 0 :this.chunkId.hashCode()));
        result = ((result* 31)+((this.seq == null)? 0 :this.seq.hashCode()));
        result = ((result* 31)+((this.ts == null)? 0 :this.ts.hashCode()));
        result = ((result* 31)+((this.token == null)? 0 :this.token.hashCode()));
        return result;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if ((other instanceof WatermarkWritten) == false) {
            return false;
        }
        WatermarkWritten rhs = ((WatermarkWritten) other);
        return ((((((((((this.sourceId == rhs.sourceId)||((this.sourceId!= null)&&this.sourceId.equals(rhs.sourceId)))&&((this.level == rhs.level)||((this.level!= null)&&this.level.equals(rhs.level))))&&((this.v == rhs.v)||((this.v!= null)&&this.v.equals(rhs.v))))&&((this.kind == rhs.kind)||((this.kind!= null)&&this.kind.equals(rhs.kind))))&&((this.runId == rhs.runId)||((this.runId!= null)&&this.runId.equals(rhs.runId))))&&((this.chunkId == rhs.chunkId)||((this.chunkId!= null)&&this.chunkId.equals(rhs.chunkId))))&&((this.seq == rhs.seq)||((this.seq!= null)&&this.seq.equals(rhs.seq))))&&((this.ts == rhs.ts)||((this.ts!= null)&&this.ts.equals(rhs.ts))))&&((this.token == rhs.token)||((this.token!= null)&&this.token.equals(rhs.token))));
    }

    public enum Kind {

        WATERMARK_WRITTEN("watermark.written");
        private final String value;
        private final static Map<String, WatermarkWritten.Kind> CONSTANTS = new HashMap<String, WatermarkWritten.Kind>();

        static {
            for (WatermarkWritten.Kind c: values()) {
                CONSTANTS.put(c.value, c);
            }
        }

        Kind(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return this.value;
        }

        @JsonValue
        public String value() {
            return this.value;
        }

        @JsonCreator
        public static WatermarkWritten.Kind fromValue(String value) {
            WatermarkWritten.Kind constant = CONSTANTS.get(value);
            if (constant == null) {
                throw new IllegalArgumentException(value);
            } else {
                return constant;
            }
        }

    }

    public enum Level {

        LOW("LOW"),
        HIGH("HIGH");
        private final String value;
        private final static Map<String, WatermarkWritten.Level> CONSTANTS = new HashMap<String, WatermarkWritten.Level>();

        static {
            for (WatermarkWritten.Level c: values()) {
                CONSTANTS.put(c.value, c);
            }
        }

        Level(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return this.value;
        }

        @JsonValue
        public String value() {
            return this.value;
        }

        @JsonCreator
        public static WatermarkWritten.Level fromValue(String value) {
            WatermarkWritten.Level constant = CONSTANTS.get(value);
            if (constant == null) {
                throw new IllegalArgumentException(value);
            } else {
                return constant;
            }
        }

    }

    public static class WatermarkWrittenBuilder
        extends WatermarkWritten.WatermarkWrittenBuilderBase<WatermarkWritten>
    {


        public WatermarkWrittenBuilder() {
            super();
        }

        public WatermarkWrittenBuilder(Long v, Instant ts, String runId, String sourceId, WatermarkWritten.Kind kind, WatermarkWritten.Level level, String token, Long chunkId) {
            super(v, ts, runId, sourceId, kind, level, token, chunkId);
        }

    }

    public static abstract class WatermarkWrittenBuilderBase<T extends WatermarkWritten >{

        protected T instance;

        @SuppressWarnings("unchecked")
        public WatermarkWrittenBuilderBase() {
            // Skip initialization when called from subclass
            if (this.getClass().equals(WatermarkWritten.WatermarkWrittenBuilder.class)) {
                this.instance = ((T) new WatermarkWritten());
            }
        }

        @SuppressWarnings("unchecked")
        public WatermarkWrittenBuilderBase(Long v, Instant ts, String runId, String sourceId, WatermarkWritten.Kind kind, WatermarkWritten.Level level, String token, Long chunkId) {
            // Skip initialization when called from subclass
            if (this.getClass().equals(WatermarkWritten.WatermarkWrittenBuilder.class)) {
                this.instance = ((T) new WatermarkWritten(v, ts, runId, sourceId, kind, level, token, chunkId));
            }
        }

        public T build() {
            T result;
            result = this.instance;
            this.instance = null;
            return result;
        }

        public WatermarkWritten.WatermarkWrittenBuilderBase withV(Long v) {
            ((WatermarkWritten) this.instance).v = v;
            return this;
        }

        public WatermarkWritten.WatermarkWrittenBuilderBase withSeq(Long seq) {
            ((WatermarkWritten) this.instance).seq = seq;
            return this;
        }

        public WatermarkWritten.WatermarkWrittenBuilderBase withTs(Instant ts) {
            ((WatermarkWritten) this.instance).ts = ts;
            return this;
        }

        public WatermarkWritten.WatermarkWrittenBuilderBase withRunId(String runId) {
            ((WatermarkWritten) this.instance).runId = runId;
            return this;
        }

        public WatermarkWritten.WatermarkWrittenBuilderBase withSourceId(String sourceId) {
            ((WatermarkWritten) this.instance).sourceId = sourceId;
            return this;
        }

        public WatermarkWritten.WatermarkWrittenBuilderBase withKind(WatermarkWritten.Kind kind) {
            ((WatermarkWritten) this.instance).kind = kind;
            return this;
        }

        public WatermarkWritten.WatermarkWrittenBuilderBase withLevel(WatermarkWritten.Level level) {
            ((WatermarkWritten) this.instance).level = level;
            return this;
        }

        public WatermarkWritten.WatermarkWrittenBuilderBase withToken(String token) {
            ((WatermarkWritten) this.instance).token = token;
            return this;
        }

        public WatermarkWritten.WatermarkWrittenBuilderBase withChunkId(Long chunkId) {
            ((WatermarkWritten) this.instance).chunkId = chunkId;
            return this;
        }

    }

}
