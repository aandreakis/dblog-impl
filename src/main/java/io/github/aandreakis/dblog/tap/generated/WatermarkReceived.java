
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
 * WatermarkReceived
 * <p>
 * The reconciler observed the LOW or HIGH watermark token come back on the source change log.
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
    "chunk_id",
    "lsn",
    "latency_ms"
})
public class WatermarkReceived {

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
    private WatermarkReceived.Kind kind;
    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("level")
    private WatermarkReceived.Level level;
    /**
     * Same token written earlier.
     * (Required)
     * 
     */
    @JsonProperty("token")
    @JsonPropertyDescription("Same token written earlier.")
    private String token;
    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("chunk_id")
    private Long chunkId;
    /**
     * LSN at which the watermark row was observed.
     * (Required)
     * 
     */
    @JsonProperty("lsn")
    @JsonPropertyDescription("LSN at which the watermark row was observed.")
    private String lsn;
    /**
     * Elapsed wall time from the matching watermark.written.
     * 
     */
    @JsonProperty("latency_ms")
    @JsonPropertyDescription("Elapsed wall time from the matching watermark.written.")
    private Long latencyMs;

    /**
     * No args constructor for use in serialization
     * 
     */
    public WatermarkReceived() {
    }

    /**
     * 
     * @param sourceId
     *     Configured dblog.source.id.
     * @param v
     *     Tap schema version; always 1.
     * @param lsn
     *     LSN at which the watermark row was observed.
     * @param runId
     *     DBLog runtime run identifier.
     * @param ts
     *     ISO-8601 UTC timestamp with microsecond resolution.
     * @param token
     *     Same token written earlier.
     */
    public WatermarkReceived(Long v, Instant ts, String runId, String sourceId, WatermarkReceived.Kind kind, WatermarkReceived.Level level, String token, Long chunkId, String lsn) {
        super();
        this.v = v;
        this.ts = ts;
        this.runId = runId;
        this.sourceId = sourceId;
        this.kind = kind;
        this.level = level;
        this.token = token;
        this.chunkId = chunkId;
        this.lsn = lsn;
    }

    public static WatermarkReceived.WatermarkReceivedBuilderBase builder() {
        return new WatermarkReceived.WatermarkReceivedBuilder();
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
    public WatermarkReceived.Kind getKind() {
        return kind;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("kind")
    public void setKind(WatermarkReceived.Kind kind) {
        this.kind = kind;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("level")
    public WatermarkReceived.Level getLevel() {
        return level;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("level")
    public void setLevel(WatermarkReceived.Level level) {
        this.level = level;
    }

    /**
     * Same token written earlier.
     * (Required)
     * 
     */
    @JsonProperty("token")
    public String getToken() {
        return token;
    }

    /**
     * Same token written earlier.
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

    /**
     * LSN at which the watermark row was observed.
     * (Required)
     * 
     */
    @JsonProperty("lsn")
    public String getLsn() {
        return lsn;
    }

    /**
     * LSN at which the watermark row was observed.
     * (Required)
     * 
     */
    @JsonProperty("lsn")
    public void setLsn(String lsn) {
        this.lsn = lsn;
    }

    /**
     * Elapsed wall time from the matching watermark.written.
     * 
     */
    @JsonProperty("latency_ms")
    public Long getLatencyMs() {
        return latencyMs;
    }

    /**
     * Elapsed wall time from the matching watermark.written.
     * 
     */
    @JsonProperty("latency_ms")
    public void setLatencyMs(Long latencyMs) {
        this.latencyMs = latencyMs;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(WatermarkReceived.class.getName()).append('@').append(Integer.toHexString(System.identityHashCode(this))).append('[');
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
        sb.append("lsn");
        sb.append('=');
        sb.append(((this.lsn == null)?"<null>":this.lsn));
        sb.append(',');
        sb.append("latencyMs");
        sb.append('=');
        sb.append(((this.latencyMs == null)?"<null>":this.latencyMs));
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
        result = ((result* 31)+((this.lsn == null)? 0 :this.lsn.hashCode()));
        result = ((result* 31)+((this.runId == null)? 0 :this.runId.hashCode()));
        result = ((result* 31)+((this.chunkId == null)? 0 :this.chunkId.hashCode()));
        result = ((result* 31)+((this.seq == null)? 0 :this.seq.hashCode()));
        result = ((result* 31)+((this.latencyMs == null)? 0 :this.latencyMs.hashCode()));
        result = ((result* 31)+((this.ts == null)? 0 :this.ts.hashCode()));
        result = ((result* 31)+((this.token == null)? 0 :this.token.hashCode()));
        return result;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if ((other instanceof WatermarkReceived) == false) {
            return false;
        }
        WatermarkReceived rhs = ((WatermarkReceived) other);
        return ((((((((((((this.sourceId == rhs.sourceId)||((this.sourceId!= null)&&this.sourceId.equals(rhs.sourceId)))&&((this.level == rhs.level)||((this.level!= null)&&this.level.equals(rhs.level))))&&((this.v == rhs.v)||((this.v!= null)&&this.v.equals(rhs.v))))&&((this.kind == rhs.kind)||((this.kind!= null)&&this.kind.equals(rhs.kind))))&&((this.lsn == rhs.lsn)||((this.lsn!= null)&&this.lsn.equals(rhs.lsn))))&&((this.runId == rhs.runId)||((this.runId!= null)&&this.runId.equals(rhs.runId))))&&((this.chunkId == rhs.chunkId)||((this.chunkId!= null)&&this.chunkId.equals(rhs.chunkId))))&&((this.seq == rhs.seq)||((this.seq!= null)&&this.seq.equals(rhs.seq))))&&((this.latencyMs == rhs.latencyMs)||((this.latencyMs!= null)&&this.latencyMs.equals(rhs.latencyMs))))&&((this.ts == rhs.ts)||((this.ts!= null)&&this.ts.equals(rhs.ts))))&&((this.token == rhs.token)||((this.token!= null)&&this.token.equals(rhs.token))));
    }

    public enum Kind {

        WATERMARK_RECEIVED("watermark.received");
        private final String value;
        private final static Map<String, WatermarkReceived.Kind> CONSTANTS = new HashMap<String, WatermarkReceived.Kind>();

        static {
            for (WatermarkReceived.Kind c: values()) {
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
        public static WatermarkReceived.Kind fromValue(String value) {
            WatermarkReceived.Kind constant = CONSTANTS.get(value);
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
        private final static Map<String, WatermarkReceived.Level> CONSTANTS = new HashMap<String, WatermarkReceived.Level>();

        static {
            for (WatermarkReceived.Level c: values()) {
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
        public static WatermarkReceived.Level fromValue(String value) {
            WatermarkReceived.Level constant = CONSTANTS.get(value);
            if (constant == null) {
                throw new IllegalArgumentException(value);
            } else {
                return constant;
            }
        }

    }

    public static class WatermarkReceivedBuilder
        extends WatermarkReceived.WatermarkReceivedBuilderBase<WatermarkReceived>
    {


        public WatermarkReceivedBuilder() {
            super();
        }

        public WatermarkReceivedBuilder(Long v, Instant ts, String runId, String sourceId, WatermarkReceived.Kind kind, WatermarkReceived.Level level, String token, Long chunkId, String lsn) {
            super(v, ts, runId, sourceId, kind, level, token, chunkId, lsn);
        }

    }

    public static abstract class WatermarkReceivedBuilderBase<T extends WatermarkReceived >{

        protected T instance;

        @SuppressWarnings("unchecked")
        public WatermarkReceivedBuilderBase() {
            // Skip initialization when called from subclass
            if (this.getClass().equals(WatermarkReceived.WatermarkReceivedBuilder.class)) {
                this.instance = ((T) new WatermarkReceived());
            }
        }

        @SuppressWarnings("unchecked")
        public WatermarkReceivedBuilderBase(Long v, Instant ts, String runId, String sourceId, WatermarkReceived.Kind kind, WatermarkReceived.Level level, String token, Long chunkId, String lsn) {
            // Skip initialization when called from subclass
            if (this.getClass().equals(WatermarkReceived.WatermarkReceivedBuilder.class)) {
                this.instance = ((T) new WatermarkReceived(v, ts, runId, sourceId, kind, level, token, chunkId, lsn));
            }
        }

        public T build() {
            T result;
            result = this.instance;
            this.instance = null;
            return result;
        }

        public WatermarkReceived.WatermarkReceivedBuilderBase withV(Long v) {
            ((WatermarkReceived) this.instance).v = v;
            return this;
        }

        public WatermarkReceived.WatermarkReceivedBuilderBase withSeq(Long seq) {
            ((WatermarkReceived) this.instance).seq = seq;
            return this;
        }

        public WatermarkReceived.WatermarkReceivedBuilderBase withTs(Instant ts) {
            ((WatermarkReceived) this.instance).ts = ts;
            return this;
        }

        public WatermarkReceived.WatermarkReceivedBuilderBase withRunId(String runId) {
            ((WatermarkReceived) this.instance).runId = runId;
            return this;
        }

        public WatermarkReceived.WatermarkReceivedBuilderBase withSourceId(String sourceId) {
            ((WatermarkReceived) this.instance).sourceId = sourceId;
            return this;
        }

        public WatermarkReceived.WatermarkReceivedBuilderBase withKind(WatermarkReceived.Kind kind) {
            ((WatermarkReceived) this.instance).kind = kind;
            return this;
        }

        public WatermarkReceived.WatermarkReceivedBuilderBase withLevel(WatermarkReceived.Level level) {
            ((WatermarkReceived) this.instance).level = level;
            return this;
        }

        public WatermarkReceived.WatermarkReceivedBuilderBase withToken(String token) {
            ((WatermarkReceived) this.instance).token = token;
            return this;
        }

        public WatermarkReceived.WatermarkReceivedBuilderBase withChunkId(Long chunkId) {
            ((WatermarkReceived) this.instance).chunkId = chunkId;
            return this;
        }

        public WatermarkReceived.WatermarkReceivedBuilderBase withLsn(String lsn) {
            ((WatermarkReceived) this.instance).lsn = lsn;
            return this;
        }

        public WatermarkReceived.WatermarkReceivedBuilderBase withLatencyMs(Long latencyMs) {
            ((WatermarkReceived) this.instance).latencyMs = latencyMs;
            return this;
        }

    }

}
