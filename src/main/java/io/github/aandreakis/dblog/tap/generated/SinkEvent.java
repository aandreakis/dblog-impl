
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
 * SinkEvent
 * <p>
 * One event appended to a sink delegate, post reconciliation. Emitted once per sink per event.
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
    "lsn",
    "op",
    "table",
    "pk",
    "origin",
    "tx_id",
    "dump_id",
    "chunk_id",
    "sink_name"
})
public class SinkEvent {

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
    private SinkEvent.Kind kind;
    /**
     * Source LSN for LOG events; HIGH watermark's LSN for SELECT refresh rows.
     * (Required)
     * 
     */
    @JsonProperty("lsn")
    @JsonPropertyDescription("Source LSN for LOG events; HIGH watermark's LSN for SELECT refresh rows.")
    private String lsn;
    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("op")
    private SinkEvent.Op op;
    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("table")
    private String table;
    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("pk")
    private String pk;
    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("origin")
    private SinkEvent.Origin origin;
    /**
     * Present on origin=LOG when available.
     * 
     */
    @JsonProperty("tx_id")
    @JsonPropertyDescription("Present on origin=LOG when available.")
    private String txId;
    /**
     * Required on origin=SELECT.
     * 
     */
    @JsonProperty("dump_id")
    @JsonPropertyDescription("Required on origin=SELECT.")
    private String dumpId;
    /**
     * Tap-assigned chunk id when the event is part of an active window.
     * 
     */
    @JsonProperty("chunk_id")
    @JsonPropertyDescription("Tap-assigned chunk id when the event is part of an active window.")
    private Long chunkId;
    /**
     * Which delegate produced this event: ndjson | jdbc | typed_h2 | noop.
     * (Required)
     * 
     */
    @JsonProperty("sink_name")
    @JsonPropertyDescription("Which delegate produced this event: ndjson | jdbc | typed_h2 | noop.")
    private String sinkName;

    /**
     * No args constructor for use in serialization
     * 
     */
    public SinkEvent() {
    }

    /**
     * 
     * @param sourceId
     *     Configured dblog.source.id.
     * @param v
     *     Tap schema version; always 1.
     * @param lsn
     *     Source LSN for LOG events; HIGH watermark's LSN for SELECT refresh rows.
     * @param runId
     *     DBLog runtime run identifier.
     * @param sinkName
     *     Which delegate produced this event: ndjson | jdbc | typed_h2 | noop.
     * @param ts
     *     ISO-8601 UTC timestamp with microsecond resolution.
     */
    public SinkEvent(Long v, Instant ts, String runId, String sourceId, SinkEvent.Kind kind, String lsn, SinkEvent.Op op, String table, String pk, SinkEvent.Origin origin, String sinkName) {
        super();
        this.v = v;
        this.ts = ts;
        this.runId = runId;
        this.sourceId = sourceId;
        this.kind = kind;
        this.lsn = lsn;
        this.op = op;
        this.table = table;
        this.pk = pk;
        this.origin = origin;
        this.sinkName = sinkName;
    }

    public static SinkEvent.SinkEventBuilderBase builder() {
        return new SinkEvent.SinkEventBuilder();
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
    public SinkEvent.Kind getKind() {
        return kind;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("kind")
    public void setKind(SinkEvent.Kind kind) {
        this.kind = kind;
    }

    /**
     * Source LSN for LOG events; HIGH watermark's LSN for SELECT refresh rows.
     * (Required)
     * 
     */
    @JsonProperty("lsn")
    public String getLsn() {
        return lsn;
    }

    /**
     * Source LSN for LOG events; HIGH watermark's LSN for SELECT refresh rows.
     * (Required)
     * 
     */
    @JsonProperty("lsn")
    public void setLsn(String lsn) {
        this.lsn = lsn;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("op")
    public SinkEvent.Op getOp() {
        return op;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("op")
    public void setOp(SinkEvent.Op op) {
        this.op = op;
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
     * 
     * (Required)
     * 
     */
    @JsonProperty("pk")
    public String getPk() {
        return pk;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("pk")
    public void setPk(String pk) {
        this.pk = pk;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("origin")
    public SinkEvent.Origin getOrigin() {
        return origin;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("origin")
    public void setOrigin(SinkEvent.Origin origin) {
        this.origin = origin;
    }

    /**
     * Present on origin=LOG when available.
     * 
     */
    @JsonProperty("tx_id")
    public String getTxId() {
        return txId;
    }

    /**
     * Present on origin=LOG when available.
     * 
     */
    @JsonProperty("tx_id")
    public void setTxId(String txId) {
        this.txId = txId;
    }

    /**
     * Required on origin=SELECT.
     * 
     */
    @JsonProperty("dump_id")
    public String getDumpId() {
        return dumpId;
    }

    /**
     * Required on origin=SELECT.
     * 
     */
    @JsonProperty("dump_id")
    public void setDumpId(String dumpId) {
        this.dumpId = dumpId;
    }

    /**
     * Tap-assigned chunk id when the event is part of an active window.
     * 
     */
    @JsonProperty("chunk_id")
    public Long getChunkId() {
        return chunkId;
    }

    /**
     * Tap-assigned chunk id when the event is part of an active window.
     * 
     */
    @JsonProperty("chunk_id")
    public void setChunkId(Long chunkId) {
        this.chunkId = chunkId;
    }

    /**
     * Which delegate produced this event: ndjson | jdbc | typed_h2 | noop.
     * (Required)
     * 
     */
    @JsonProperty("sink_name")
    public String getSinkName() {
        return sinkName;
    }

    /**
     * Which delegate produced this event: ndjson | jdbc | typed_h2 | noop.
     * (Required)
     * 
     */
    @JsonProperty("sink_name")
    public void setSinkName(String sinkName) {
        this.sinkName = sinkName;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(SinkEvent.class.getName()).append('@').append(Integer.toHexString(System.identityHashCode(this))).append('[');
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
        sb.append("lsn");
        sb.append('=');
        sb.append(((this.lsn == null)?"<null>":this.lsn));
        sb.append(',');
        sb.append("op");
        sb.append('=');
        sb.append(((this.op == null)?"<null>":this.op));
        sb.append(',');
        sb.append("table");
        sb.append('=');
        sb.append(((this.table == null)?"<null>":this.table));
        sb.append(',');
        sb.append("pk");
        sb.append('=');
        sb.append(((this.pk == null)?"<null>":this.pk));
        sb.append(',');
        sb.append("origin");
        sb.append('=');
        sb.append(((this.origin == null)?"<null>":this.origin));
        sb.append(',');
        sb.append("txId");
        sb.append('=');
        sb.append(((this.txId == null)?"<null>":this.txId));
        sb.append(',');
        sb.append("dumpId");
        sb.append('=');
        sb.append(((this.dumpId == null)?"<null>":this.dumpId));
        sb.append(',');
        sb.append("chunkId");
        sb.append('=');
        sb.append(((this.chunkId == null)?"<null>":this.chunkId));
        sb.append(',');
        sb.append("sinkName");
        sb.append('=');
        sb.append(((this.sinkName == null)?"<null>":this.sinkName));
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
        result = ((result* 31)+((this.op == null)? 0 :this.op.hashCode()));
        result = ((result* 31)+((this.kind == null)? 0 :this.kind.hashCode()));
        result = ((result* 31)+((this.lsn == null)? 0 :this.lsn.hashCode()));
        result = ((result* 31)+((this.origin == null)? 0 :this.origin.hashCode()));
        result = ((result* 31)+((this.dumpId == null)? 0 :this.dumpId.hashCode()));
        result = ((result* 31)+((this.txId == null)? 0 :this.txId.hashCode()));
        result = ((result* 31)+((this.chunkId == null)? 0 :this.chunkId.hashCode()));
        result = ((result* 31)+((this.sinkName == null)? 0 :this.sinkName.hashCode()));
        result = ((result* 31)+((this.v == null)? 0 :this.v.hashCode()));
        result = ((result* 31)+((this.runId == null)? 0 :this.runId.hashCode()));
        result = ((result* 31)+((this.pk == null)? 0 :this.pk.hashCode()));
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
        if ((other instanceof SinkEvent) == false) {
            return false;
        }
        SinkEvent rhs = ((SinkEvent) other);
        return ((((((((((((((((this.sourceId == rhs.sourceId)||((this.sourceId!= null)&&this.sourceId.equals(rhs.sourceId)))&&((this.op == rhs.op)||((this.op!= null)&&this.op.equals(rhs.op))))&&((this.kind == rhs.kind)||((this.kind!= null)&&this.kind.equals(rhs.kind))))&&((this.lsn == rhs.lsn)||((this.lsn!= null)&&this.lsn.equals(rhs.lsn))))&&((this.origin == rhs.origin)||((this.origin!= null)&&this.origin.equals(rhs.origin))))&&((this.dumpId == rhs.dumpId)||((this.dumpId!= null)&&this.dumpId.equals(rhs.dumpId))))&&((this.txId == rhs.txId)||((this.txId!= null)&&this.txId.equals(rhs.txId))))&&((this.chunkId == rhs.chunkId)||((this.chunkId!= null)&&this.chunkId.equals(rhs.chunkId))))&&((this.sinkName == rhs.sinkName)||((this.sinkName!= null)&&this.sinkName.equals(rhs.sinkName))))&&((this.v == rhs.v)||((this.v!= null)&&this.v.equals(rhs.v))))&&((this.runId == rhs.runId)||((this.runId!= null)&&this.runId.equals(rhs.runId))))&&((this.pk == rhs.pk)||((this.pk!= null)&&this.pk.equals(rhs.pk))))&&((this.seq == rhs.seq)||((this.seq!= null)&&this.seq.equals(rhs.seq))))&&((this.table == rhs.table)||((this.table!= null)&&this.table.equals(rhs.table))))&&((this.ts == rhs.ts)||((this.ts!= null)&&this.ts.equals(rhs.ts))));
    }

    public enum Kind {

        SINK_EVENT("sink.event");
        private final String value;
        private final static Map<String, SinkEvent.Kind> CONSTANTS = new HashMap<String, SinkEvent.Kind>();

        static {
            for (SinkEvent.Kind c: values()) {
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
        public static SinkEvent.Kind fromValue(String value) {
            SinkEvent.Kind constant = CONSTANTS.get(value);
            if (constant == null) {
                throw new IllegalArgumentException(value);
            } else {
                return constant;
            }
        }

    }

    public enum Op {

        INSERT("INSERT"),
        UPDATE("UPDATE"),
        DELETE("DELETE");
        private final String value;
        private final static Map<String, SinkEvent.Op> CONSTANTS = new HashMap<String, SinkEvent.Op>();

        static {
            for (SinkEvent.Op c: values()) {
                CONSTANTS.put(c.value, c);
            }
        }

        Op(String value) {
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
        public static SinkEvent.Op fromValue(String value) {
            SinkEvent.Op constant = CONSTANTS.get(value);
            if (constant == null) {
                throw new IllegalArgumentException(value);
            } else {
                return constant;
            }
        }

    }

    public enum Origin {

        LOG("LOG"),
        SELECT("SELECT");
        private final String value;
        private final static Map<String, SinkEvent.Origin> CONSTANTS = new HashMap<String, SinkEvent.Origin>();

        static {
            for (SinkEvent.Origin c: values()) {
                CONSTANTS.put(c.value, c);
            }
        }

        Origin(String value) {
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
        public static SinkEvent.Origin fromValue(String value) {
            SinkEvent.Origin constant = CONSTANTS.get(value);
            if (constant == null) {
                throw new IllegalArgumentException(value);
            } else {
                return constant;
            }
        }

    }

    public static class SinkEventBuilder
        extends SinkEvent.SinkEventBuilderBase<SinkEvent>
    {


        public SinkEventBuilder() {
            super();
        }

        public SinkEventBuilder(Long v, Instant ts, String runId, String sourceId, SinkEvent.Kind kind, String lsn, SinkEvent.Op op, String table, String pk, SinkEvent.Origin origin, String sinkName) {
            super(v, ts, runId, sourceId, kind, lsn, op, table, pk, origin, sinkName);
        }

    }

    public static abstract class SinkEventBuilderBase<T extends SinkEvent >{

        protected T instance;

        @SuppressWarnings("unchecked")
        public SinkEventBuilderBase() {
            // Skip initialization when called from subclass
            if (this.getClass().equals(SinkEvent.SinkEventBuilder.class)) {
                this.instance = ((T) new SinkEvent());
            }
        }

        @SuppressWarnings("unchecked")
        public SinkEventBuilderBase(Long v, Instant ts, String runId, String sourceId, SinkEvent.Kind kind, String lsn, SinkEvent.Op op, String table, String pk, SinkEvent.Origin origin, String sinkName) {
            // Skip initialization when called from subclass
            if (this.getClass().equals(SinkEvent.SinkEventBuilder.class)) {
                this.instance = ((T) new SinkEvent(v, ts, runId, sourceId, kind, lsn, op, table, pk, origin, sinkName));
            }
        }

        public T build() {
            T result;
            result = this.instance;
            this.instance = null;
            return result;
        }

        public SinkEvent.SinkEventBuilderBase withV(Long v) {
            ((SinkEvent) this.instance).v = v;
            return this;
        }

        public SinkEvent.SinkEventBuilderBase withSeq(Long seq) {
            ((SinkEvent) this.instance).seq = seq;
            return this;
        }

        public SinkEvent.SinkEventBuilderBase withTs(Instant ts) {
            ((SinkEvent) this.instance).ts = ts;
            return this;
        }

        public SinkEvent.SinkEventBuilderBase withRunId(String runId) {
            ((SinkEvent) this.instance).runId = runId;
            return this;
        }

        public SinkEvent.SinkEventBuilderBase withSourceId(String sourceId) {
            ((SinkEvent) this.instance).sourceId = sourceId;
            return this;
        }

        public SinkEvent.SinkEventBuilderBase withKind(SinkEvent.Kind kind) {
            ((SinkEvent) this.instance).kind = kind;
            return this;
        }

        public SinkEvent.SinkEventBuilderBase withLsn(String lsn) {
            ((SinkEvent) this.instance).lsn = lsn;
            return this;
        }

        public SinkEvent.SinkEventBuilderBase withOp(SinkEvent.Op op) {
            ((SinkEvent) this.instance).op = op;
            return this;
        }

        public SinkEvent.SinkEventBuilderBase withTable(String table) {
            ((SinkEvent) this.instance).table = table;
            return this;
        }

        public SinkEvent.SinkEventBuilderBase withPk(String pk) {
            ((SinkEvent) this.instance).pk = pk;
            return this;
        }

        public SinkEvent.SinkEventBuilderBase withOrigin(SinkEvent.Origin origin) {
            ((SinkEvent) this.instance).origin = origin;
            return this;
        }

        public SinkEvent.SinkEventBuilderBase withTxId(String txId) {
            ((SinkEvent) this.instance).txId = txId;
            return this;
        }

        public SinkEvent.SinkEventBuilderBase withDumpId(String dumpId) {
            ((SinkEvent) this.instance).dumpId = dumpId;
            return this;
        }

        public SinkEvent.SinkEventBuilderBase withChunkId(Long chunkId) {
            ((SinkEvent) this.instance).chunkId = chunkId;
            return this;
        }

        public SinkEvent.SinkEventBuilderBase withSinkName(String sinkName) {
            ((SinkEvent) this.instance).sinkName = sinkName;
            return this;
        }

    }

}
