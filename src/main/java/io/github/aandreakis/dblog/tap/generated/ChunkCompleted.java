
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
 * ChunkCompleted
 * <p>
 * HIGH watermark observed, refresh rows emitted, chunk acknowledged.
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
    "chunk_id",
    "request_id",
    "table",
    "emitted",
    "excluded",
    "last_pk",
    "lsn",
    "duration_ms",
    "final_chunk"
})
public class ChunkCompleted {

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
    private ChunkCompleted.Kind kind;
    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("chunk_id")
    private Long chunkId;
    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("request_id")
    private String requestId;
    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("table")
    private String table;
    /**
     * Refresh SELECT rows released on HIGH watermark.
     * (Required)
     * 
     */
    @JsonProperty("emitted")
    @JsonPropertyDescription("Refresh SELECT rows released on HIGH watermark.")
    private Long emitted;
    /**
     * Rows dropped due to in-window collisions.
     * (Required)
     * 
     */
    @JsonProperty("excluded")
    @JsonPropertyDescription("Rows dropped due to in-window collisions.")
    private Long excluded;
    /**
     * Chunk's lastPrimaryKey. Absent when the chunk has no last-primary-key tuple.
     * 
     */
    @JsonProperty("last_pk")
    @JsonPropertyDescription("Chunk's lastPrimaryKey. Absent when the chunk has no last-primary-key tuple.")
    private String lastPk;
    /**
     * HIGH watermark's LSN — the refresh rows share it.
     * (Required)
     * 
     */
    @JsonProperty("lsn")
    @JsonPropertyDescription("HIGH watermark's LSN \u2014 the refresh rows share it.")
    private String lsn;
    /**
     * Wall time from watermark.written LOW to this event.
     * (Required)
     * 
     */
    @JsonProperty("duration_ms")
    @JsonPropertyDescription("Wall time from watermark.written LOW to this event.")
    private Long durationMs;
    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("final_chunk")
    private Boolean finalChunk;

    /**
     * No args constructor for use in serialization
     * 
     */
    public ChunkCompleted() {
    }

    /**
     * 
     * @param sourceId
     *     Configured dblog.source.id.
     * @param excluded
     *     Rows dropped due to in-window collisions.
     * @param emitted
     *     Refresh SELECT rows released on HIGH watermark.
     * @param v
     *     Tap schema version; always 1.
     * @param lsn
     *     HIGH watermark's LSN — the refresh rows share it.
     * @param runId
     *     DBLog runtime run identifier.
     * @param durationMs
     *     Wall time from watermark.written LOW to this event.
     * @param ts
     *     ISO-8601 UTC timestamp with microsecond resolution.
     */
    public ChunkCompleted(Long v, Instant ts, String runId, String sourceId, ChunkCompleted.Kind kind, Long chunkId, String requestId, String table, Long emitted, Long excluded, String lsn, Long durationMs, Boolean finalChunk) {
        super();
        this.v = v;
        this.ts = ts;
        this.runId = runId;
        this.sourceId = sourceId;
        this.kind = kind;
        this.chunkId = chunkId;
        this.requestId = requestId;
        this.table = table;
        this.emitted = emitted;
        this.excluded = excluded;
        this.lsn = lsn;
        this.durationMs = durationMs;
        this.finalChunk = finalChunk;
    }

    public static ChunkCompleted.ChunkCompletedBuilderBase builder() {
        return new ChunkCompleted.ChunkCompletedBuilder();
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
    public ChunkCompleted.Kind getKind() {
        return kind;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("kind")
    public void setKind(ChunkCompleted.Kind kind) {
        this.kind = kind;
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
     * 
     * (Required)
     * 
     */
    @JsonProperty("request_id")
    public String getRequestId() {
        return requestId;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("request_id")
    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("table")
    public String getTable() {
        return table;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("table")
    public void setTable(String table) {
        this.table = table;
    }

    /**
     * Refresh SELECT rows released on HIGH watermark.
     * (Required)
     * 
     */
    @JsonProperty("emitted")
    public Long getEmitted() {
        return emitted;
    }

    /**
     * Refresh SELECT rows released on HIGH watermark.
     * (Required)
     * 
     */
    @JsonProperty("emitted")
    public void setEmitted(Long emitted) {
        this.emitted = emitted;
    }

    /**
     * Rows dropped due to in-window collisions.
     * (Required)
     * 
     */
    @JsonProperty("excluded")
    public Long getExcluded() {
        return excluded;
    }

    /**
     * Rows dropped due to in-window collisions.
     * (Required)
     * 
     */
    @JsonProperty("excluded")
    public void setExcluded(Long excluded) {
        this.excluded = excluded;
    }

    /**
     * Chunk's lastPrimaryKey. Absent when the chunk has no last-primary-key tuple.
     * 
     */
    @JsonProperty("last_pk")
    public String getLastPk() {
        return lastPk;
    }

    /**
     * Chunk's lastPrimaryKey. Absent when the chunk has no last-primary-key tuple.
     * 
     */
    @JsonProperty("last_pk")
    public void setLastPk(String lastPk) {
        this.lastPk = lastPk;
    }

    /**
     * HIGH watermark's LSN — the refresh rows share it.
     * (Required)
     * 
     */
    @JsonProperty("lsn")
    public String getLsn() {
        return lsn;
    }

    /**
     * HIGH watermark's LSN — the refresh rows share it.
     * (Required)
     * 
     */
    @JsonProperty("lsn")
    public void setLsn(String lsn) {
        this.lsn = lsn;
    }

    /**
     * Wall time from watermark.written LOW to this event.
     * (Required)
     * 
     */
    @JsonProperty("duration_ms")
    public Long getDurationMs() {
        return durationMs;
    }

    /**
     * Wall time from watermark.written LOW to this event.
     * (Required)
     * 
     */
    @JsonProperty("duration_ms")
    public void setDurationMs(Long durationMs) {
        this.durationMs = durationMs;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("final_chunk")
    public Boolean getFinalChunk() {
        return finalChunk;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("final_chunk")
    public void setFinalChunk(Boolean finalChunk) {
        this.finalChunk = finalChunk;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(ChunkCompleted.class.getName()).append('@').append(Integer.toHexString(System.identityHashCode(this))).append('[');
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
        sb.append("chunkId");
        sb.append('=');
        sb.append(((this.chunkId == null)?"<null>":this.chunkId));
        sb.append(',');
        sb.append("requestId");
        sb.append('=');
        sb.append(((this.requestId == null)?"<null>":this.requestId));
        sb.append(',');
        sb.append("table");
        sb.append('=');
        sb.append(((this.table == null)?"<null>":this.table));
        sb.append(',');
        sb.append("emitted");
        sb.append('=');
        sb.append(((this.emitted == null)?"<null>":this.emitted));
        sb.append(',');
        sb.append("excluded");
        sb.append('=');
        sb.append(((this.excluded == null)?"<null>":this.excluded));
        sb.append(',');
        sb.append("lastPk");
        sb.append('=');
        sb.append(((this.lastPk == null)?"<null>":this.lastPk));
        sb.append(',');
        sb.append("lsn");
        sb.append('=');
        sb.append(((this.lsn == null)?"<null>":this.lsn));
        sb.append(',');
        sb.append("durationMs");
        sb.append('=');
        sb.append(((this.durationMs == null)?"<null>":this.durationMs));
        sb.append(',');
        sb.append("finalChunk");
        sb.append('=');
        sb.append(((this.finalChunk == null)?"<null>":this.finalChunk));
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
        result = ((result* 31)+((this.kind == null)? 0 :this.kind.hashCode()));
        result = ((result* 31)+((this.lsn == null)? 0 :this.lsn.hashCode()));
        result = ((result* 31)+((this.lastPk == null)? 0 :this.lastPk.hashCode()));
        result = ((result* 31)+((this.finalChunk == null)? 0 :this.finalChunk.hashCode()));
        result = ((result* 31)+((this.chunkId == null)? 0 :this.chunkId.hashCode()));
        result = ((result* 31)+((this.excluded == null)? 0 :this.excluded.hashCode()));
        result = ((result* 31)+((this.emitted == null)? 0 :this.emitted.hashCode()));
        result = ((result* 31)+((this.v == null)? 0 :this.v.hashCode()));
        result = ((result* 31)+((this.requestId == null)? 0 :this.requestId.hashCode()));
        result = ((result* 31)+((this.runId == null)? 0 :this.runId.hashCode()));
        result = ((result* 31)+((this.durationMs == null)? 0 :this.durationMs.hashCode()));
        result = ((result* 31)+((this.seq == null)? 0 :this.seq.hashCode()));
        result = ((result* 31)+((this.table == null)? 0 :this.table.hashCode()));
        result = ((result* 31)+((this.ts == null)? 0 :this.ts.hashCode()));
        return result;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if ((other instanceof ChunkCompleted) == false) {
            return false;
        }
        ChunkCompleted rhs = ((ChunkCompleted) other);
        return ((((((((((((((((this.sourceId == rhs.sourceId)||((this.sourceId!= null)&&this.sourceId.equals(rhs.sourceId)))&&((this.kind == rhs.kind)||((this.kind!= null)&&this.kind.equals(rhs.kind))))&&((this.lsn == rhs.lsn)||((this.lsn!= null)&&this.lsn.equals(rhs.lsn))))&&((this.lastPk == rhs.lastPk)||((this.lastPk!= null)&&this.lastPk.equals(rhs.lastPk))))&&((this.finalChunk == rhs.finalChunk)||((this.finalChunk!= null)&&this.finalChunk.equals(rhs.finalChunk))))&&((this.chunkId == rhs.chunkId)||((this.chunkId!= null)&&this.chunkId.equals(rhs.chunkId))))&&((this.excluded == rhs.excluded)||((this.excluded!= null)&&this.excluded.equals(rhs.excluded))))&&((this.emitted == rhs.emitted)||((this.emitted!= null)&&this.emitted.equals(rhs.emitted))))&&((this.v == rhs.v)||((this.v!= null)&&this.v.equals(rhs.v))))&&((this.requestId == rhs.requestId)||((this.requestId!= null)&&this.requestId.equals(rhs.requestId))))&&((this.runId == rhs.runId)||((this.runId!= null)&&this.runId.equals(rhs.runId))))&&((this.durationMs == rhs.durationMs)||((this.durationMs!= null)&&this.durationMs.equals(rhs.durationMs))))&&((this.seq == rhs.seq)||((this.seq!= null)&&this.seq.equals(rhs.seq))))&&((this.table == rhs.table)||((this.table!= null)&&this.table.equals(rhs.table))))&&((this.ts == rhs.ts)||((this.ts!= null)&&this.ts.equals(rhs.ts))));
    }

    public static class ChunkCompletedBuilder
        extends ChunkCompleted.ChunkCompletedBuilderBase<ChunkCompleted>
    {


        public ChunkCompletedBuilder() {
            super();
        }

        public ChunkCompletedBuilder(Long v, Instant ts, String runId, String sourceId, ChunkCompleted.Kind kind, Long chunkId, String requestId, String table, Long emitted, Long excluded, String lsn, Long durationMs, Boolean finalChunk) {
            super(v, ts, runId, sourceId, kind, chunkId, requestId, table, emitted, excluded, lsn, durationMs, finalChunk);
        }

    }

    public static abstract class ChunkCompletedBuilderBase<T extends ChunkCompleted >{

        protected T instance;

        @SuppressWarnings("unchecked")
        public ChunkCompletedBuilderBase() {
            // Skip initialization when called from subclass
            if (this.getClass().equals(ChunkCompleted.ChunkCompletedBuilder.class)) {
                this.instance = ((T) new ChunkCompleted());
            }
        }

        @SuppressWarnings("unchecked")
        public ChunkCompletedBuilderBase(Long v, Instant ts, String runId, String sourceId, ChunkCompleted.Kind kind, Long chunkId, String requestId, String table, Long emitted, Long excluded, String lsn, Long durationMs, Boolean finalChunk) {
            // Skip initialization when called from subclass
            if (this.getClass().equals(ChunkCompleted.ChunkCompletedBuilder.class)) {
                this.instance = ((T) new ChunkCompleted(v, ts, runId, sourceId, kind, chunkId, requestId, table, emitted, excluded, lsn, durationMs, finalChunk));
            }
        }

        public T build() {
            T result;
            result = this.instance;
            this.instance = null;
            return result;
        }

        public ChunkCompleted.ChunkCompletedBuilderBase withV(Long v) {
            ((ChunkCompleted) this.instance).v = v;
            return this;
        }

        public ChunkCompleted.ChunkCompletedBuilderBase withSeq(Long seq) {
            ((ChunkCompleted) this.instance).seq = seq;
            return this;
        }

        public ChunkCompleted.ChunkCompletedBuilderBase withTs(Instant ts) {
            ((ChunkCompleted) this.instance).ts = ts;
            return this;
        }

        public ChunkCompleted.ChunkCompletedBuilderBase withRunId(String runId) {
            ((ChunkCompleted) this.instance).runId = runId;
            return this;
        }

        public ChunkCompleted.ChunkCompletedBuilderBase withSourceId(String sourceId) {
            ((ChunkCompleted) this.instance).sourceId = sourceId;
            return this;
        }

        public ChunkCompleted.ChunkCompletedBuilderBase withKind(ChunkCompleted.Kind kind) {
            ((ChunkCompleted) this.instance).kind = kind;
            return this;
        }

        public ChunkCompleted.ChunkCompletedBuilderBase withChunkId(Long chunkId) {
            ((ChunkCompleted) this.instance).chunkId = chunkId;
            return this;
        }

        public ChunkCompleted.ChunkCompletedBuilderBase withRequestId(String requestId) {
            ((ChunkCompleted) this.instance).requestId = requestId;
            return this;
        }

        public ChunkCompleted.ChunkCompletedBuilderBase withTable(String table) {
            ((ChunkCompleted) this.instance).table = table;
            return this;
        }

        public ChunkCompleted.ChunkCompletedBuilderBase withEmitted(Long emitted) {
            ((ChunkCompleted) this.instance).emitted = emitted;
            return this;
        }

        public ChunkCompleted.ChunkCompletedBuilderBase withExcluded(Long excluded) {
            ((ChunkCompleted) this.instance).excluded = excluded;
            return this;
        }

        public ChunkCompleted.ChunkCompletedBuilderBase withLastPk(String lastPk) {
            ((ChunkCompleted) this.instance).lastPk = lastPk;
            return this;
        }

        public ChunkCompleted.ChunkCompletedBuilderBase withLsn(String lsn) {
            ((ChunkCompleted) this.instance).lsn = lsn;
            return this;
        }

        public ChunkCompleted.ChunkCompletedBuilderBase withDurationMs(Long durationMs) {
            ((ChunkCompleted) this.instance).durationMs = durationMs;
            return this;
        }

        public ChunkCompleted.ChunkCompletedBuilderBase withFinalChunk(Boolean finalChunk) {
            ((ChunkCompleted) this.instance).finalChunk = finalChunk;
            return this;
        }

    }

    public enum Kind {

        CHUNK_COMPLETED("chunk.completed");
        private final String value;
        private final static Map<String, ChunkCompleted.Kind> CONSTANTS = new HashMap<String, ChunkCompleted.Kind>();

        static {
            for (ChunkCompleted.Kind c: values()) {
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
        public static ChunkCompleted.Kind fromValue(String value) {
            ChunkCompleted.Kind constant = CONSTANTS.get(value);
            if (constant == null) {
                throw new IllegalArgumentException(value);
            } else {
                return constant;
            }
        }

    }

}
