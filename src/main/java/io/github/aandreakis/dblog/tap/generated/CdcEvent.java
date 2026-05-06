
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
 * CdcEvent
 * <p>
 * One CDC row change observed on the source change stream.
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
    "tx_id"
})
public class CdcEvent {

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
    private CdcEvent.Kind kind;
    /**
     * Source position display value.
     * (Required)
     * 
     */
    @JsonProperty("lsn")
    @JsonPropertyDescription("Source position display value.")
    private String lsn;
    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("op")
    private CdcEvent.Op op;
    /**
     * Display name, e.g. "app.orders".
     * (Required)
     * 
     */
    @JsonProperty("table")
    @JsonPropertyDescription("Display name, e.g. \"app.orders\".")
    private String table;
    /**
     * Canonical primary-key literal.
     * (Required)
     * 
     */
    @JsonProperty("pk")
    @JsonPropertyDescription("Canonical primary-key literal.")
    private String pk;
    /**
     * Source transaction id when available.
     * 
     */
    @JsonProperty("tx_id")
    @JsonPropertyDescription("Source transaction id when available.")
    private String txId;

    /**
     * No args constructor for use in serialization
     * 
     */
    public CdcEvent() {
    }

    /**
     * 
     * @param sourceId
     *     Configured dblog.source.id.
     * @param v
     *     Tap schema version; always 1.
     * @param lsn
     *     Source position display value.
     * @param runId
     *     DBLog runtime run identifier.
     * @param pk
     *     Canonical primary-key literal.
     * @param table
     *     Display name, e.g. "app.orders".
     * @param ts
     *     ISO-8601 UTC timestamp with microsecond resolution.
     */
    public CdcEvent(Long v, Instant ts, String runId, String sourceId, CdcEvent.Kind kind, String lsn, CdcEvent.Op op, String table, String pk) {
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
    }

    public static CdcEvent.CdcEventBuilderBase builder() {
        return new CdcEvent.CdcEventBuilder();
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
    public CdcEvent.Kind getKind() {
        return kind;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("kind")
    public void setKind(CdcEvent.Kind kind) {
        this.kind = kind;
    }

    /**
     * Source position display value.
     * (Required)
     * 
     */
    @JsonProperty("lsn")
    public String getLsn() {
        return lsn;
    }

    /**
     * Source position display value.
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
    public CdcEvent.Op getOp() {
        return op;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("op")
    public void setOp(CdcEvent.Op op) {
        this.op = op;
    }

    /**
     * Display name, e.g. "app.orders".
     * (Required)
     * 
     */
    @JsonProperty("table")
    public String getTable() {
        return table;
    }

    /**
     * Display name, e.g. "app.orders".
     * (Required)
     * 
     */
    @JsonProperty("table")
    public void setTable(String table) {
        this.table = table;
    }

    /**
     * Canonical primary-key literal.
     * (Required)
     * 
     */
    @JsonProperty("pk")
    public String getPk() {
        return pk;
    }

    /**
     * Canonical primary-key literal.
     * (Required)
     * 
     */
    @JsonProperty("pk")
    public void setPk(String pk) {
        this.pk = pk;
    }

    /**
     * Source transaction id when available.
     * 
     */
    @JsonProperty("tx_id")
    public String getTxId() {
        return txId;
    }

    /**
     * Source transaction id when available.
     * 
     */
    @JsonProperty("tx_id")
    public void setTxId(String txId) {
        this.txId = txId;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(CdcEvent.class.getName()).append('@').append(Integer.toHexString(System.identityHashCode(this))).append('[');
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
        sb.append("txId");
        sb.append('=');
        sb.append(((this.txId == null)?"<null>":this.txId));
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
        result = ((result* 31)+((this.v == null)? 0 :this.v.hashCode()));
        result = ((result* 31)+((this.kind == null)? 0 :this.kind.hashCode()));
        result = ((result* 31)+((this.lsn == null)? 0 :this.lsn.hashCode()));
        result = ((result* 31)+((this.txId == null)? 0 :this.txId.hashCode()));
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
        if ((other instanceof CdcEvent) == false) {
            return false;
        }
        CdcEvent rhs = ((CdcEvent) other);
        return ((((((((((((this.sourceId == rhs.sourceId)||((this.sourceId!= null)&&this.sourceId.equals(rhs.sourceId)))&&((this.op == rhs.op)||((this.op!= null)&&this.op.equals(rhs.op))))&&((this.v == rhs.v)||((this.v!= null)&&this.v.equals(rhs.v))))&&((this.kind == rhs.kind)||((this.kind!= null)&&this.kind.equals(rhs.kind))))&&((this.lsn == rhs.lsn)||((this.lsn!= null)&&this.lsn.equals(rhs.lsn))))&&((this.txId == rhs.txId)||((this.txId!= null)&&this.txId.equals(rhs.txId))))&&((this.runId == rhs.runId)||((this.runId!= null)&&this.runId.equals(rhs.runId))))&&((this.pk == rhs.pk)||((this.pk!= null)&&this.pk.equals(rhs.pk))))&&((this.seq == rhs.seq)||((this.seq!= null)&&this.seq.equals(rhs.seq))))&&((this.table == rhs.table)||((this.table!= null)&&this.table.equals(rhs.table))))&&((this.ts == rhs.ts)||((this.ts!= null)&&this.ts.equals(rhs.ts))));
    }

    public static class CdcEventBuilder
        extends CdcEvent.CdcEventBuilderBase<CdcEvent>
    {


        public CdcEventBuilder() {
            super();
        }

        public CdcEventBuilder(Long v, Instant ts, String runId, String sourceId, CdcEvent.Kind kind, String lsn, CdcEvent.Op op, String table, String pk) {
            super(v, ts, runId, sourceId, kind, lsn, op, table, pk);
        }

    }

    public static abstract class CdcEventBuilderBase<T extends CdcEvent >{

        protected T instance;

        @SuppressWarnings("unchecked")
        public CdcEventBuilderBase() {
            // Skip initialization when called from subclass
            if (this.getClass().equals(CdcEvent.CdcEventBuilder.class)) {
                this.instance = ((T) new CdcEvent());
            }
        }

        @SuppressWarnings("unchecked")
        public CdcEventBuilderBase(Long v, Instant ts, String runId, String sourceId, CdcEvent.Kind kind, String lsn, CdcEvent.Op op, String table, String pk) {
            // Skip initialization when called from subclass
            if (this.getClass().equals(CdcEvent.CdcEventBuilder.class)) {
                this.instance = ((T) new CdcEvent(v, ts, runId, sourceId, kind, lsn, op, table, pk));
            }
        }

        public T build() {
            T result;
            result = this.instance;
            this.instance = null;
            return result;
        }

        public CdcEvent.CdcEventBuilderBase withV(Long v) {
            ((CdcEvent) this.instance).v = v;
            return this;
        }

        public CdcEvent.CdcEventBuilderBase withSeq(Long seq) {
            ((CdcEvent) this.instance).seq = seq;
            return this;
        }

        public CdcEvent.CdcEventBuilderBase withTs(Instant ts) {
            ((CdcEvent) this.instance).ts = ts;
            return this;
        }

        public CdcEvent.CdcEventBuilderBase withRunId(String runId) {
            ((CdcEvent) this.instance).runId = runId;
            return this;
        }

        public CdcEvent.CdcEventBuilderBase withSourceId(String sourceId) {
            ((CdcEvent) this.instance).sourceId = sourceId;
            return this;
        }

        public CdcEvent.CdcEventBuilderBase withKind(CdcEvent.Kind kind) {
            ((CdcEvent) this.instance).kind = kind;
            return this;
        }

        public CdcEvent.CdcEventBuilderBase withLsn(String lsn) {
            ((CdcEvent) this.instance).lsn = lsn;
            return this;
        }

        public CdcEvent.CdcEventBuilderBase withOp(CdcEvent.Op op) {
            ((CdcEvent) this.instance).op = op;
            return this;
        }

        public CdcEvent.CdcEventBuilderBase withTable(String table) {
            ((CdcEvent) this.instance).table = table;
            return this;
        }

        public CdcEvent.CdcEventBuilderBase withPk(String pk) {
            ((CdcEvent) this.instance).pk = pk;
            return this;
        }

        public CdcEvent.CdcEventBuilderBase withTxId(String txId) {
            ((CdcEvent) this.instance).txId = txId;
            return this;
        }

    }

    public enum Kind {

        CDC("cdc");
        private final String value;
        private final static Map<String, CdcEvent.Kind> CONSTANTS = new HashMap<String, CdcEvent.Kind>();

        static {
            for (CdcEvent.Kind c: values()) {
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
        public static CdcEvent.Kind fromValue(String value) {
            CdcEvent.Kind constant = CONSTANTS.get(value);
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
        private final static Map<String, CdcEvent.Op> CONSTANTS = new HashMap<String, CdcEvent.Op>();

        static {
            for (CdcEvent.Op c: values()) {
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
        public static CdcEvent.Op fromValue(String value) {
            CdcEvent.Op constant = CONSTANTS.get(value);
            if (constant == null) {
                throw new IllegalArgumentException(value);
            } else {
                return constant;
            }
        }

    }

}
