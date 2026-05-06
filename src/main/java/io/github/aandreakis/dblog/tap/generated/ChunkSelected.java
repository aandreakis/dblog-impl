
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
 * ChunkSelected
 * <p>
 * A chunk SELECT finished inside the currently open watermark window.
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
    "dump_id",
    "table",
    "mode",
    "pk_min",
    "pk_max",
    "start_after_pk",
    "row_count",
    "final_chunk",
    "fingerprint"
})
public class ChunkSelected {

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
    private ChunkSelected.Kind kind;
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
     * Stamped on refresh rows as ChangeEvent.dumpId.
     * (Required)
     * 
     */
    @JsonProperty("dump_id")
    @JsonPropertyDescription("Stamped on refresh rows as ChangeEvent.dumpId.")
    private String dumpId;
    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("table")
    private String table;
    /**
     * Always "range" in the current core.
     * (Required)
     * 
     */
    @JsonProperty("mode")
    @JsonPropertyDescription("Always \"range\" in the current core.")
    private ChunkSelected.Mode mode;
    /**
     * Lower bound PK literal. Absent when the chunk is empty.
     * 
     */
    @JsonProperty("pk_min")
    @JsonPropertyDescription("Lower bound PK literal. Absent when the chunk is empty.")
    private String pkMin;
    /**
     * Upper bound PK literal. Absent when the chunk has no last-primary-key tuple.
     * 
     */
    @JsonProperty("pk_max")
    @JsonPropertyDescription("Upper bound PK literal. Absent when the chunk has no last-primary-key tuple.")
    private String pkMax;
    /**
     * Previous chunk's last PK. Absent on the first chunk.
     * 
     */
    @JsonProperty("start_after_pk")
    @JsonPropertyDescription("Previous chunk's last PK. Absent on the first chunk.")
    private String startAfterPk;
    /**
     * Rows the SELECT returned.
     * (Required)
     * 
     */
    @JsonProperty("row_count")
    @JsonPropertyDescription("Rows the SELECT returned.")
    private Long rowCount;
    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("final_chunk")
    private Boolean finalChunk;
    /**
     * TableSchema.fingerprint() — identifies the schema revision.
     * (Required)
     * 
     */
    @JsonProperty("fingerprint")
    @JsonPropertyDescription("TableSchema.fingerprint() \u2014 identifies the schema revision.")
    private String fingerprint;

    /**
     * No args constructor for use in serialization
     * 
     */
    public ChunkSelected() {
    }

    /**
     * 
     * @param sourceId
     *     Configured dblog.source.id.
     * @param mode
     *     Always "range" in the current core.
     * @param v
     *     Tap schema version; always 1.
     * @param dumpId
     *     Stamped on refresh rows as ChangeEvent.dumpId.
     * @param fingerprint
     *     TableSchema.fingerprint() — identifies the schema revision.
     * @param runId
     *     DBLog runtime run identifier.
     * @param rowCount
     *     Rows the SELECT returned.
     * @param ts
     *     ISO-8601 UTC timestamp with microsecond resolution.
     */
    public ChunkSelected(Long v, Instant ts, String runId, String sourceId, ChunkSelected.Kind kind, Long chunkId, String requestId, String dumpId, String table, ChunkSelected.Mode mode, Long rowCount, Boolean finalChunk, String fingerprint) {
        super();
        this.v = v;
        this.ts = ts;
        this.runId = runId;
        this.sourceId = sourceId;
        this.kind = kind;
        this.chunkId = chunkId;
        this.requestId = requestId;
        this.dumpId = dumpId;
        this.table = table;
        this.mode = mode;
        this.rowCount = rowCount;
        this.finalChunk = finalChunk;
        this.fingerprint = fingerprint;
    }

    public static ChunkSelected.ChunkSelectedBuilderBase builder() {
        return new ChunkSelected.ChunkSelectedBuilder();
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
    public ChunkSelected.Kind getKind() {
        return kind;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("kind")
    public void setKind(ChunkSelected.Kind kind) {
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
     * Stamped on refresh rows as ChangeEvent.dumpId.
     * (Required)
     * 
     */
    @JsonProperty("dump_id")
    public String getDumpId() {
        return dumpId;
    }

    /**
     * Stamped on refresh rows as ChangeEvent.dumpId.
     * (Required)
     * 
     */
    @JsonProperty("dump_id")
    public void setDumpId(String dumpId) {
        this.dumpId = dumpId;
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
     * Always "range" in the current core.
     * (Required)
     * 
     */
    @JsonProperty("mode")
    public ChunkSelected.Mode getMode() {
        return mode;
    }

    /**
     * Always "range" in the current core.
     * (Required)
     * 
     */
    @JsonProperty("mode")
    public void setMode(ChunkSelected.Mode mode) {
        this.mode = mode;
    }

    /**
     * Lower bound PK literal. Absent when the chunk is empty.
     * 
     */
    @JsonProperty("pk_min")
    public String getPkMin() {
        return pkMin;
    }

    /**
     * Lower bound PK literal. Absent when the chunk is empty.
     * 
     */
    @JsonProperty("pk_min")
    public void setPkMin(String pkMin) {
        this.pkMin = pkMin;
    }

    /**
     * Upper bound PK literal. Absent when the chunk has no last-primary-key tuple.
     * 
     */
    @JsonProperty("pk_max")
    public String getPkMax() {
        return pkMax;
    }

    /**
     * Upper bound PK literal. Absent when the chunk has no last-primary-key tuple.
     * 
     */
    @JsonProperty("pk_max")
    public void setPkMax(String pkMax) {
        this.pkMax = pkMax;
    }

    /**
     * Previous chunk's last PK. Absent on the first chunk.
     * 
     */
    @JsonProperty("start_after_pk")
    public String getStartAfterPk() {
        return startAfterPk;
    }

    /**
     * Previous chunk's last PK. Absent on the first chunk.
     * 
     */
    @JsonProperty("start_after_pk")
    public void setStartAfterPk(String startAfterPk) {
        this.startAfterPk = startAfterPk;
    }

    /**
     * Rows the SELECT returned.
     * (Required)
     * 
     */
    @JsonProperty("row_count")
    public Long getRowCount() {
        return rowCount;
    }

    /**
     * Rows the SELECT returned.
     * (Required)
     * 
     */
    @JsonProperty("row_count")
    public void setRowCount(Long rowCount) {
        this.rowCount = rowCount;
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

    /**
     * TableSchema.fingerprint() — identifies the schema revision.
     * (Required)
     * 
     */
    @JsonProperty("fingerprint")
    public String getFingerprint() {
        return fingerprint;
    }

    /**
     * TableSchema.fingerprint() — identifies the schema revision.
     * (Required)
     * 
     */
    @JsonProperty("fingerprint")
    public void setFingerprint(String fingerprint) {
        this.fingerprint = fingerprint;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(ChunkSelected.class.getName()).append('@').append(Integer.toHexString(System.identityHashCode(this))).append('[');
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
        sb.append("dumpId");
        sb.append('=');
        sb.append(((this.dumpId == null)?"<null>":this.dumpId));
        sb.append(',');
        sb.append("table");
        sb.append('=');
        sb.append(((this.table == null)?"<null>":this.table));
        sb.append(',');
        sb.append("mode");
        sb.append('=');
        sb.append(((this.mode == null)?"<null>":this.mode));
        sb.append(',');
        sb.append("pkMin");
        sb.append('=');
        sb.append(((this.pkMin == null)?"<null>":this.pkMin));
        sb.append(',');
        sb.append("pkMax");
        sb.append('=');
        sb.append(((this.pkMax == null)?"<null>":this.pkMax));
        sb.append(',');
        sb.append("startAfterPk");
        sb.append('=');
        sb.append(((this.startAfterPk == null)?"<null>":this.startAfterPk));
        sb.append(',');
        sb.append("rowCount");
        sb.append('=');
        sb.append(((this.rowCount == null)?"<null>":this.rowCount));
        sb.append(',');
        sb.append("finalChunk");
        sb.append('=');
        sb.append(((this.finalChunk == null)?"<null>":this.finalChunk));
        sb.append(',');
        sb.append("fingerprint");
        sb.append('=');
        sb.append(((this.fingerprint == null)?"<null>":this.fingerprint));
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
        result = ((result* 31)+((this.dumpId == null)? 0 :this.dumpId.hashCode()));
        result = ((result* 31)+((this.finalChunk == null)? 0 :this.finalChunk.hashCode()));
        result = ((result* 31)+((this.pkMin == null)? 0 :this.pkMin.hashCode()));
        result = ((result* 31)+((this.startAfterPk == null)? 0 :this.startAfterPk.hashCode()));
        result = ((result* 31)+((this.chunkId == null)? 0 :this.chunkId.hashCode()));
        result = ((result* 31)+((this.mode == null)? 0 :this.mode.hashCode()));
        result = ((result* 31)+((this.pkMax == null)? 0 :this.pkMax.hashCode()));
        result = ((result* 31)+((this.v == null)? 0 :this.v.hashCode()));
        result = ((result* 31)+((this.requestId == null)? 0 :this.requestId.hashCode()));
        result = ((result* 31)+((this.fingerprint == null)? 0 :this.fingerprint.hashCode()));
        result = ((result* 31)+((this.runId == null)? 0 :this.runId.hashCode()));
        result = ((result* 31)+((this.rowCount == null)? 0 :this.rowCount.hashCode()));
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
        if ((other instanceof ChunkSelected) == false) {
            return false;
        }
        ChunkSelected rhs = ((ChunkSelected) other);
        return ((((((((((((((((((this.sourceId == rhs.sourceId)||((this.sourceId!= null)&&this.sourceId.equals(rhs.sourceId)))&&((this.kind == rhs.kind)||((this.kind!= null)&&this.kind.equals(rhs.kind))))&&((this.dumpId == rhs.dumpId)||((this.dumpId!= null)&&this.dumpId.equals(rhs.dumpId))))&&((this.finalChunk == rhs.finalChunk)||((this.finalChunk!= null)&&this.finalChunk.equals(rhs.finalChunk))))&&((this.pkMin == rhs.pkMin)||((this.pkMin!= null)&&this.pkMin.equals(rhs.pkMin))))&&((this.startAfterPk == rhs.startAfterPk)||((this.startAfterPk!= null)&&this.startAfterPk.equals(rhs.startAfterPk))))&&((this.chunkId == rhs.chunkId)||((this.chunkId!= null)&&this.chunkId.equals(rhs.chunkId))))&&((this.mode == rhs.mode)||((this.mode!= null)&&this.mode.equals(rhs.mode))))&&((this.pkMax == rhs.pkMax)||((this.pkMax!= null)&&this.pkMax.equals(rhs.pkMax))))&&((this.v == rhs.v)||((this.v!= null)&&this.v.equals(rhs.v))))&&((this.requestId == rhs.requestId)||((this.requestId!= null)&&this.requestId.equals(rhs.requestId))))&&((this.fingerprint == rhs.fingerprint)||((this.fingerprint!= null)&&this.fingerprint.equals(rhs.fingerprint))))&&((this.runId == rhs.runId)||((this.runId!= null)&&this.runId.equals(rhs.runId))))&&((this.rowCount == rhs.rowCount)||((this.rowCount!= null)&&this.rowCount.equals(rhs.rowCount))))&&((this.seq == rhs.seq)||((this.seq!= null)&&this.seq.equals(rhs.seq))))&&((this.table == rhs.table)||((this.table!= null)&&this.table.equals(rhs.table))))&&((this.ts == rhs.ts)||((this.ts!= null)&&this.ts.equals(rhs.ts))));
    }

    public static class ChunkSelectedBuilder
        extends ChunkSelected.ChunkSelectedBuilderBase<ChunkSelected>
    {


        public ChunkSelectedBuilder() {
            super();
        }

        public ChunkSelectedBuilder(Long v, Instant ts, String runId, String sourceId, ChunkSelected.Kind kind, Long chunkId, String requestId, String dumpId, String table, ChunkSelected.Mode mode, Long rowCount, Boolean finalChunk, String fingerprint) {
            super(v, ts, runId, sourceId, kind, chunkId, requestId, dumpId, table, mode, rowCount, finalChunk, fingerprint);
        }

    }

    public static abstract class ChunkSelectedBuilderBase<T extends ChunkSelected >{

        protected T instance;

        @SuppressWarnings("unchecked")
        public ChunkSelectedBuilderBase() {
            // Skip initialization when called from subclass
            if (this.getClass().equals(ChunkSelected.ChunkSelectedBuilder.class)) {
                this.instance = ((T) new ChunkSelected());
            }
        }

        @SuppressWarnings("unchecked")
        public ChunkSelectedBuilderBase(Long v, Instant ts, String runId, String sourceId, ChunkSelected.Kind kind, Long chunkId, String requestId, String dumpId, String table, ChunkSelected.Mode mode, Long rowCount, Boolean finalChunk, String fingerprint) {
            // Skip initialization when called from subclass
            if (this.getClass().equals(ChunkSelected.ChunkSelectedBuilder.class)) {
                this.instance = ((T) new ChunkSelected(v, ts, runId, sourceId, kind, chunkId, requestId, dumpId, table, mode, rowCount, finalChunk, fingerprint));
            }
        }

        public T build() {
            T result;
            result = this.instance;
            this.instance = null;
            return result;
        }

        public ChunkSelected.ChunkSelectedBuilderBase withV(Long v) {
            ((ChunkSelected) this.instance).v = v;
            return this;
        }

        public ChunkSelected.ChunkSelectedBuilderBase withSeq(Long seq) {
            ((ChunkSelected) this.instance).seq = seq;
            return this;
        }

        public ChunkSelected.ChunkSelectedBuilderBase withTs(Instant ts) {
            ((ChunkSelected) this.instance).ts = ts;
            return this;
        }

        public ChunkSelected.ChunkSelectedBuilderBase withRunId(String runId) {
            ((ChunkSelected) this.instance).runId = runId;
            return this;
        }

        public ChunkSelected.ChunkSelectedBuilderBase withSourceId(String sourceId) {
            ((ChunkSelected) this.instance).sourceId = sourceId;
            return this;
        }

        public ChunkSelected.ChunkSelectedBuilderBase withKind(ChunkSelected.Kind kind) {
            ((ChunkSelected) this.instance).kind = kind;
            return this;
        }

        public ChunkSelected.ChunkSelectedBuilderBase withChunkId(Long chunkId) {
            ((ChunkSelected) this.instance).chunkId = chunkId;
            return this;
        }

        public ChunkSelected.ChunkSelectedBuilderBase withRequestId(String requestId) {
            ((ChunkSelected) this.instance).requestId = requestId;
            return this;
        }

        public ChunkSelected.ChunkSelectedBuilderBase withDumpId(String dumpId) {
            ((ChunkSelected) this.instance).dumpId = dumpId;
            return this;
        }

        public ChunkSelected.ChunkSelectedBuilderBase withTable(String table) {
            ((ChunkSelected) this.instance).table = table;
            return this;
        }

        public ChunkSelected.ChunkSelectedBuilderBase withMode(ChunkSelected.Mode mode) {
            ((ChunkSelected) this.instance).mode = mode;
            return this;
        }

        public ChunkSelected.ChunkSelectedBuilderBase withPkMin(String pkMin) {
            ((ChunkSelected) this.instance).pkMin = pkMin;
            return this;
        }

        public ChunkSelected.ChunkSelectedBuilderBase withPkMax(String pkMax) {
            ((ChunkSelected) this.instance).pkMax = pkMax;
            return this;
        }

        public ChunkSelected.ChunkSelectedBuilderBase withStartAfterPk(String startAfterPk) {
            ((ChunkSelected) this.instance).startAfterPk = startAfterPk;
            return this;
        }

        public ChunkSelected.ChunkSelectedBuilderBase withRowCount(Long rowCount) {
            ((ChunkSelected) this.instance).rowCount = rowCount;
            return this;
        }

        public ChunkSelected.ChunkSelectedBuilderBase withFinalChunk(Boolean finalChunk) {
            ((ChunkSelected) this.instance).finalChunk = finalChunk;
            return this;
        }

        public ChunkSelected.ChunkSelectedBuilderBase withFingerprint(String fingerprint) {
            ((ChunkSelected) this.instance).fingerprint = fingerprint;
            return this;
        }

    }

    public enum Kind {

        CHUNK_SELECTED("chunk.selected");
        private final String value;
        private final static Map<String, ChunkSelected.Kind> CONSTANTS = new HashMap<String, ChunkSelected.Kind>();

        static {
            for (ChunkSelected.Kind c: values()) {
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
        public static ChunkSelected.Kind fromValue(String value) {
            ChunkSelected.Kind constant = CONSTANTS.get(value);
            if (constant == null) {
                throw new IllegalArgumentException(value);
            } else {
                return constant;
            }
        }

    }


    /**
     * Always "range" in the current core.
     * 
     */
    public enum Mode {

        RANGE("range");
        private final String value;
        private final static Map<String, ChunkSelected.Mode> CONSTANTS = new HashMap<String, ChunkSelected.Mode>();

        static {
            for (ChunkSelected.Mode c: values()) {
                CONSTANTS.put(c.value, c);
            }
        }

        Mode(String value) {
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
        public static ChunkSelected.Mode fromValue(String value) {
            ChunkSelected.Mode constant = CONSTANTS.get(value);
            if (constant == null) {
                throw new IllegalArgumentException(value);
            } else {
                return constant;
            }
        }

    }

}
