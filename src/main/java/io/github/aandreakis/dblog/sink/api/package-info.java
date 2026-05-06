/**
 * Sink port for emitted neutral events. Implementations choose their durability and idempotency
 * model; {@link io.github.aandreakis.dblog.sink.api.ChangeEventSink} is the central
 * contract.
 */
package io.github.aandreakis.dblog.sink.api;
