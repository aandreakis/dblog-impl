/**
 * Durable state-store ports DBLog needs across restarts.
 * {@link io.github.aandreakis.dblog.state.api.RuntimeStateStore} is the aggregate root;
 * the per-concern repositories (stream positions, dump requests, dump progress, schemas, source
 * ownership) live alongside it.
 */
package io.github.aandreakis.dblog.state.api;
