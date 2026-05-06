
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
 * CheckpointAdvanced
 * <p>
 * Buffered streaming checkpoint rolled forward.
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
    "position",
    "buffered_events",
    "reason"
})
public class CheckpointAdvanced {

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
    private CheckpointAdvanced.Kind kind;
    /**
     * New durable checkpoint LSN.
     * (Required)
     * 
     */
    @JsonProperty("position")
    @JsonPropertyDescription("New durable checkpoint LSN.")
    private String position;
    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("buffered_events")
    private Long bufferedEvents;
    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("reason")
    private CheckpointAdvanced.Reason reason;

    /**
     * No args constructor for use in serialization
     * 
     */
    public CheckpointAdvanced() {
    }

    /**
     * 
     * @param sourceId
     *     Configured dblog.source.id.
     * @param v
     *     Tap schema version; always 1.
     * @param runId
     *     DBLog runtime run identifier.
     * @param position
     *     New durable checkpoint LSN.
     * @param ts
     *     ISO-8601 UTC timestamp with microsecond resolution.
     */
    public CheckpointAdvanced(Long v, Instant ts, String runId, String sourceId, CheckpointAdvanced.Kind kind, String position, Long bufferedEvents, CheckpointAdvanced.Reason reason) {
        super();
        this.v = v;
        this.ts = ts;
        this.runId = runId;
        this.sourceId = sourceId;
        this.kind = kind;
        this.position = position;
        this.bufferedEvents = bufferedEvents;
        this.reason = reason;
    }

    public static CheckpointAdvanced.CheckpointAdvancedBuilderBase builder() {
        return new CheckpointAdvanced.CheckpointAdvancedBuilder();
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
    public CheckpointAdvanced.Kind getKind() {
        return kind;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("kind")
    public void setKind(CheckpointAdvanced.Kind kind) {
        this.kind = kind;
    }

    /**
     * New durable checkpoint LSN.
     * (Required)
     * 
     */
    @JsonProperty("position")
    public String getPosition() {
        return position;
    }

    /**
     * New durable checkpoint LSN.
     * (Required)
     * 
     */
    @JsonProperty("position")
    public void setPosition(String position) {
        this.position = position;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("buffered_events")
    public Long getBufferedEvents() {
        return bufferedEvents;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("buffered_events")
    public void setBufferedEvents(Long bufferedEvents) {
        this.bufferedEvents = bufferedEvents;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("reason")
    public CheckpointAdvanced.Reason getReason() {
        return reason;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("reason")
    public void setReason(CheckpointAdvanced.Reason reason) {
        this.reason = reason;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(CheckpointAdvanced.class.getName()).append('@').append(Integer.toHexString(System.identityHashCode(this))).append('[');
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
        sb.append("position");
        sb.append('=');
        sb.append(((this.position == null)?"<null>":this.position));
        sb.append(',');
        sb.append("bufferedEvents");
        sb.append('=');
        sb.append(((this.bufferedEvents == null)?"<null>":this.bufferedEvents));
        sb.append(',');
        sb.append("reason");
        sb.append('=');
        sb.append(((this.reason == null)?"<null>":this.reason));
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
        result = ((result* 31)+((this.reason == null)? 0 :this.reason.hashCode()));
        result = ((result* 31)+((this.v == null)? 0 :this.v.hashCode()));
        result = ((result* 31)+((this.kind == null)? 0 :this.kind.hashCode()));
        result = ((result* 31)+((this.runId == null)? 0 :this.runId.hashCode()));
        result = ((result* 31)+((this.position == null)? 0 :this.position.hashCode()));
        result = ((result* 31)+((this.bufferedEvents == null)? 0 :this.bufferedEvents.hashCode()));
        result = ((result* 31)+((this.seq == null)? 0 :this.seq.hashCode()));
        result = ((result* 31)+((this.ts == null)? 0 :this.ts.hashCode()));
        return result;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if ((other instanceof CheckpointAdvanced) == false) {
            return false;
        }
        CheckpointAdvanced rhs = ((CheckpointAdvanced) other);
        return ((((((((((this.sourceId == rhs.sourceId)||((this.sourceId!= null)&&this.sourceId.equals(rhs.sourceId)))&&((this.reason == rhs.reason)||((this.reason!= null)&&this.reason.equals(rhs.reason))))&&((this.v == rhs.v)||((this.v!= null)&&this.v.equals(rhs.v))))&&((this.kind == rhs.kind)||((this.kind!= null)&&this.kind.equals(rhs.kind))))&&((this.runId == rhs.runId)||((this.runId!= null)&&this.runId.equals(rhs.runId))))&&((this.position == rhs.position)||((this.position!= null)&&this.position.equals(rhs.position))))&&((this.bufferedEvents == rhs.bufferedEvents)||((this.bufferedEvents!= null)&&this.bufferedEvents.equals(rhs.bufferedEvents))))&&((this.seq == rhs.seq)||((this.seq!= null)&&this.seq.equals(rhs.seq))))&&((this.ts == rhs.ts)||((this.ts!= null)&&this.ts.equals(rhs.ts))));
    }

    public static class CheckpointAdvancedBuilder
        extends CheckpointAdvanced.CheckpointAdvancedBuilderBase<CheckpointAdvanced>
    {


        public CheckpointAdvancedBuilder() {
            super();
        }

        public CheckpointAdvancedBuilder(Long v, Instant ts, String runId, String sourceId, CheckpointAdvanced.Kind kind, String position, Long bufferedEvents, CheckpointAdvanced.Reason reason) {
            super(v, ts, runId, sourceId, kind, position, bufferedEvents, reason);
        }

    }

    public static abstract class CheckpointAdvancedBuilderBase<T extends CheckpointAdvanced >{

        protected T instance;

        @SuppressWarnings("unchecked")
        public CheckpointAdvancedBuilderBase() {
            // Skip initialization when called from subclass
            if (this.getClass().equals(CheckpointAdvanced.CheckpointAdvancedBuilder.class)) {
                this.instance = ((T) new CheckpointAdvanced());
            }
        }

        @SuppressWarnings("unchecked")
        public CheckpointAdvancedBuilderBase(Long v, Instant ts, String runId, String sourceId, CheckpointAdvanced.Kind kind, String position, Long bufferedEvents, CheckpointAdvanced.Reason reason) {
            // Skip initialization when called from subclass
            if (this.getClass().equals(CheckpointAdvanced.CheckpointAdvancedBuilder.class)) {
                this.instance = ((T) new CheckpointAdvanced(v, ts, runId, sourceId, kind, position, bufferedEvents, reason));
            }
        }

        public T build() {
            T result;
            result = this.instance;
            this.instance = null;
            return result;
        }

        public CheckpointAdvanced.CheckpointAdvancedBuilderBase withV(Long v) {
            ((CheckpointAdvanced) this.instance).v = v;
            return this;
        }

        public CheckpointAdvanced.CheckpointAdvancedBuilderBase withSeq(Long seq) {
            ((CheckpointAdvanced) this.instance).seq = seq;
            return this;
        }

        public CheckpointAdvanced.CheckpointAdvancedBuilderBase withTs(Instant ts) {
            ((CheckpointAdvanced) this.instance).ts = ts;
            return this;
        }

        public CheckpointAdvanced.CheckpointAdvancedBuilderBase withRunId(String runId) {
            ((CheckpointAdvanced) this.instance).runId = runId;
            return this;
        }

        public CheckpointAdvanced.CheckpointAdvancedBuilderBase withSourceId(String sourceId) {
            ((CheckpointAdvanced) this.instance).sourceId = sourceId;
            return this;
        }

        public CheckpointAdvanced.CheckpointAdvancedBuilderBase withKind(CheckpointAdvanced.Kind kind) {
            ((CheckpointAdvanced) this.instance).kind = kind;
            return this;
        }

        public CheckpointAdvanced.CheckpointAdvancedBuilderBase withPosition(String position) {
            ((CheckpointAdvanced) this.instance).position = position;
            return this;
        }

        public CheckpointAdvanced.CheckpointAdvancedBuilderBase withBufferedEvents(Long bufferedEvents) {
            ((CheckpointAdvanced) this.instance).bufferedEvents = bufferedEvents;
            return this;
        }

        public CheckpointAdvanced.CheckpointAdvancedBuilderBase withReason(CheckpointAdvanced.Reason reason) {
            ((CheckpointAdvanced) this.instance).reason = reason;
            return this;
        }

    }

    public enum Kind {

        CHECKPOINT_ADVANCED("checkpoint.advanced");
        private final String value;
        private final static Map<String, CheckpointAdvanced.Kind> CONSTANTS = new HashMap<String, CheckpointAdvanced.Kind>();

        static {
            for (CheckpointAdvanced.Kind c: values()) {
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
        public static CheckpointAdvanced.Kind fromValue(String value) {
            CheckpointAdvanced.Kind constant = CONSTANTS.get(value);
            if (constant == null) {
                throw new IllegalArgumentException(value);
            } else {
                return constant;
            }
        }

    }

    public enum Reason {

        COUNT("count"),
        TIME("time"),
        BYTES("bytes"),
        BOUNDARY("boundary");
        private final String value;
        private final static Map<String, CheckpointAdvanced.Reason> CONSTANTS = new HashMap<String, CheckpointAdvanced.Reason>();

        static {
            for (CheckpointAdvanced.Reason c: values()) {
                CONSTANTS.put(c.value, c);
            }
        }

        Reason(String value) {
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
        public static CheckpointAdvanced.Reason fromValue(String value) {
            CheckpointAdvanced.Reason constant = CONSTANTS.get(value);
            if (constant == null) {
                throw new IllegalArgumentException(value);
            } else {
                return constant;
            }
        }

    }

}
