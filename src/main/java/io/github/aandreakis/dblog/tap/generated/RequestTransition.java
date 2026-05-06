
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
 * RequestTransition
 * <p>
 * A dump request's durable state changed.
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
    "request_id",
    "scope",
    "table",
    "state",
    "prev_state",
    "reason"
})
public class RequestTransition {

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
    private RequestTransition.Kind kind;
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
    @JsonProperty("scope")
    private RequestTransition.Scope scope;
    /**
     * Required for TABLE and PRIMARY_KEYS; absent for ALL_TABLES.
     * 
     */
    @JsonProperty("table")
    @JsonPropertyDescription("Required for TABLE and PRIMARY_KEYS; absent for ALL_TABLES.")
    private String table;
    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("state")
    private RequestTransition.State state;
    /**
     * Absent on the first-ever transition.
     * 
     */
    @JsonProperty("prev_state")
    @JsonPropertyDescription("Absent on the first-ever transition.")
    private RequestTransition.PrevState prevState;
    /**
     * Populated when state=FAILED.
     * 
     */
    @JsonProperty("reason")
    @JsonPropertyDescription("Populated when state=FAILED.")
    private String reason;

    /**
     * No args constructor for use in serialization
     * 
     */
    public RequestTransition() {
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
     */
    public RequestTransition(Long v, Instant ts, String runId, String sourceId, RequestTransition.Kind kind, String requestId, RequestTransition.Scope scope, RequestTransition.State state) {
        super();
        this.v = v;
        this.ts = ts;
        this.runId = runId;
        this.sourceId = sourceId;
        this.kind = kind;
        this.requestId = requestId;
        this.scope = scope;
        this.state = state;
    }

    public static RequestTransition.RequestTransitionBuilderBase builder() {
        return new RequestTransition.RequestTransitionBuilder();
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
    public RequestTransition.Kind getKind() {
        return kind;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("kind")
    public void setKind(RequestTransition.Kind kind) {
        this.kind = kind;
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
    @JsonProperty("scope")
    public RequestTransition.Scope getScope() {
        return scope;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("scope")
    public void setScope(RequestTransition.Scope scope) {
        this.scope = scope;
    }

    /**
     * Required for TABLE and PRIMARY_KEYS; absent for ALL_TABLES.
     * 
     */
    @JsonProperty("table")
    public String getTable() {
        return table;
    }

    /**
     * Required for TABLE and PRIMARY_KEYS; absent for ALL_TABLES.
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
    @JsonProperty("state")
    public RequestTransition.State getState() {
        return state;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("state")
    public void setState(RequestTransition.State state) {
        this.state = state;
    }

    /**
     * Absent on the first-ever transition.
     * 
     */
    @JsonProperty("prev_state")
    public RequestTransition.PrevState getPrevState() {
        return prevState;
    }

    /**
     * Absent on the first-ever transition.
     * 
     */
    @JsonProperty("prev_state")
    public void setPrevState(RequestTransition.PrevState prevState) {
        this.prevState = prevState;
    }

    /**
     * Populated when state=FAILED.
     * 
     */
    @JsonProperty("reason")
    public String getReason() {
        return reason;
    }

    /**
     * Populated when state=FAILED.
     * 
     */
    @JsonProperty("reason")
    public void setReason(String reason) {
        this.reason = reason;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(RequestTransition.class.getName()).append('@').append(Integer.toHexString(System.identityHashCode(this))).append('[');
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
        sb.append("requestId");
        sb.append('=');
        sb.append(((this.requestId == null)?"<null>":this.requestId));
        sb.append(',');
        sb.append("scope");
        sb.append('=');
        sb.append(((this.scope == null)?"<null>":this.scope));
        sb.append(',');
        sb.append("table");
        sb.append('=');
        sb.append(((this.table == null)?"<null>":this.table));
        sb.append(',');
        sb.append("state");
        sb.append('=');
        sb.append(((this.state == null)?"<null>":this.state));
        sb.append(',');
        sb.append("prevState");
        sb.append('=');
        sb.append(((this.prevState == null)?"<null>":this.prevState));
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
        result = ((result* 31)+((this.prevState == null)? 0 :this.prevState.hashCode()));
        result = ((result* 31)+((this.reason == null)? 0 :this.reason.hashCode()));
        result = ((result* 31)+((this.v == null)? 0 :this.v.hashCode()));
        result = ((result* 31)+((this.kind == null)? 0 :this.kind.hashCode()));
        result = ((result* 31)+((this.requestId == null)? 0 :this.requestId.hashCode()));
        result = ((result* 31)+((this.scope == null)? 0 :this.scope.hashCode()));
        result = ((result* 31)+((this.runId == null)? 0 :this.runId.hashCode()));
        result = ((result* 31)+((this.state == null)? 0 :this.state.hashCode()));
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
        if ((other instanceof RequestTransition) == false) {
            return false;
        }
        RequestTransition rhs = ((RequestTransition) other);
        return (((((((((((((this.sourceId == rhs.sourceId)||((this.sourceId!= null)&&this.sourceId.equals(rhs.sourceId)))&&((this.prevState == rhs.prevState)||((this.prevState!= null)&&this.prevState.equals(rhs.prevState))))&&((this.reason == rhs.reason)||((this.reason!= null)&&this.reason.equals(rhs.reason))))&&((this.v == rhs.v)||((this.v!= null)&&this.v.equals(rhs.v))))&&((this.kind == rhs.kind)||((this.kind!= null)&&this.kind.equals(rhs.kind))))&&((this.requestId == rhs.requestId)||((this.requestId!= null)&&this.requestId.equals(rhs.requestId))))&&((this.scope == rhs.scope)||((this.scope!= null)&&this.scope.equals(rhs.scope))))&&((this.runId == rhs.runId)||((this.runId!= null)&&this.runId.equals(rhs.runId))))&&((this.state == rhs.state)||((this.state!= null)&&this.state.equals(rhs.state))))&&((this.seq == rhs.seq)||((this.seq!= null)&&this.seq.equals(rhs.seq))))&&((this.table == rhs.table)||((this.table!= null)&&this.table.equals(rhs.table))))&&((this.ts == rhs.ts)||((this.ts!= null)&&this.ts.equals(rhs.ts))));
    }

    public enum Kind {

        REQUEST_TRANSITION("request.transition");
        private final String value;
        private final static Map<String, RequestTransition.Kind> CONSTANTS = new HashMap<String, RequestTransition.Kind>();

        static {
            for (RequestTransition.Kind c: values()) {
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
        public static RequestTransition.Kind fromValue(String value) {
            RequestTransition.Kind constant = CONSTANTS.get(value);
            if (constant == null) {
                throw new IllegalArgumentException(value);
            } else {
                return constant;
            }
        }

    }


    /**
     * Absent on the first-ever transition.
     * 
     */
    public enum PrevState {

        ACTIVE("ACTIVE"),
        COMPLETED("COMPLETED"),
        FAILED("FAILED");
        private final String value;
        private final static Map<String, RequestTransition.PrevState> CONSTANTS = new HashMap<String, RequestTransition.PrevState>();

        static {
            for (RequestTransition.PrevState c: values()) {
                CONSTANTS.put(c.value, c);
            }
        }

        PrevState(String value) {
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
        public static RequestTransition.PrevState fromValue(String value) {
            RequestTransition.PrevState constant = CONSTANTS.get(value);
            if (constant == null) {
                throw new IllegalArgumentException(value);
            } else {
                return constant;
            }
        }

    }

    public static class RequestTransitionBuilder
        extends RequestTransition.RequestTransitionBuilderBase<RequestTransition>
    {


        public RequestTransitionBuilder() {
            super();
        }

        public RequestTransitionBuilder(Long v, Instant ts, String runId, String sourceId, RequestTransition.Kind kind, String requestId, RequestTransition.Scope scope, RequestTransition.State state) {
            super(v, ts, runId, sourceId, kind, requestId, scope, state);
        }

    }

    public static abstract class RequestTransitionBuilderBase<T extends RequestTransition >{

        protected T instance;

        @SuppressWarnings("unchecked")
        public RequestTransitionBuilderBase() {
            // Skip initialization when called from subclass
            if (this.getClass().equals(RequestTransition.RequestTransitionBuilder.class)) {
                this.instance = ((T) new RequestTransition());
            }
        }

        @SuppressWarnings("unchecked")
        public RequestTransitionBuilderBase(Long v, Instant ts, String runId, String sourceId, RequestTransition.Kind kind, String requestId, RequestTransition.Scope scope, RequestTransition.State state) {
            // Skip initialization when called from subclass
            if (this.getClass().equals(RequestTransition.RequestTransitionBuilder.class)) {
                this.instance = ((T) new RequestTransition(v, ts, runId, sourceId, kind, requestId, scope, state));
            }
        }

        public T build() {
            T result;
            result = this.instance;
            this.instance = null;
            return result;
        }

        public RequestTransition.RequestTransitionBuilderBase withV(Long v) {
            ((RequestTransition) this.instance).v = v;
            return this;
        }

        public RequestTransition.RequestTransitionBuilderBase withSeq(Long seq) {
            ((RequestTransition) this.instance).seq = seq;
            return this;
        }

        public RequestTransition.RequestTransitionBuilderBase withTs(Instant ts) {
            ((RequestTransition) this.instance).ts = ts;
            return this;
        }

        public RequestTransition.RequestTransitionBuilderBase withRunId(String runId) {
            ((RequestTransition) this.instance).runId = runId;
            return this;
        }

        public RequestTransition.RequestTransitionBuilderBase withSourceId(String sourceId) {
            ((RequestTransition) this.instance).sourceId = sourceId;
            return this;
        }

        public RequestTransition.RequestTransitionBuilderBase withKind(RequestTransition.Kind kind) {
            ((RequestTransition) this.instance).kind = kind;
            return this;
        }

        public RequestTransition.RequestTransitionBuilderBase withRequestId(String requestId) {
            ((RequestTransition) this.instance).requestId = requestId;
            return this;
        }

        public RequestTransition.RequestTransitionBuilderBase withScope(RequestTransition.Scope scope) {
            ((RequestTransition) this.instance).scope = scope;
            return this;
        }

        public RequestTransition.RequestTransitionBuilderBase withTable(String table) {
            ((RequestTransition) this.instance).table = table;
            return this;
        }

        public RequestTransition.RequestTransitionBuilderBase withState(RequestTransition.State state) {
            ((RequestTransition) this.instance).state = state;
            return this;
        }

        public RequestTransition.RequestTransitionBuilderBase withPrevState(RequestTransition.PrevState prevState) {
            ((RequestTransition) this.instance).prevState = prevState;
            return this;
        }

        public RequestTransition.RequestTransitionBuilderBase withReason(String reason) {
            ((RequestTransition) this.instance).reason = reason;
            return this;
        }

    }

    public enum Scope {

        TABLE("TABLE"),
        PRIMARY_KEYS("PRIMARY_KEYS"),
        ALL_TABLES("ALL_TABLES");
        private final String value;
        private final static Map<String, RequestTransition.Scope> CONSTANTS = new HashMap<String, RequestTransition.Scope>();

        static {
            for (RequestTransition.Scope c: values()) {
                CONSTANTS.put(c.value, c);
            }
        }

        Scope(String value) {
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
        public static RequestTransition.Scope fromValue(String value) {
            RequestTransition.Scope constant = CONSTANTS.get(value);
            if (constant == null) {
                throw new IllegalArgumentException(value);
            } else {
                return constant;
            }
        }

    }

    public enum State {

        ACTIVE("ACTIVE"),
        COMPLETED("COMPLETED"),
        FAILED("FAILED");
        private final String value;
        private final static Map<String, RequestTransition.State> CONSTANTS = new HashMap<String, RequestTransition.State>();

        static {
            for (RequestTransition.State c: values()) {
                CONSTANTS.put(c.value, c);
            }
        }

        State(String value) {
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
        public static RequestTransition.State fromValue(String value) {
            RequestTransition.State constant = CONSTANTS.get(value);
            if (constant == null) {
                throw new IllegalArgumentException(value);
            } else {
                return constant;
            }
        }

    }

}
