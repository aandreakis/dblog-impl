/**
 * Source-adapter port and supporting types. A source adapter implements
 * {@link io.github.aandreakis.dblog.adapter.api.SourceAdapter} to plug a concrete
 * database (MySQL, PostgreSQL) into the DBLog runtime. The semantic core never touches an adapter
 * directly; it goes through narrow ports defined here:
 * {@link io.github.aandreakis.dblog.adapter.api.BoundChunkReader},
 * {@link io.github.aandreakis.dblog.adapter.api.SourceTransaction},
 * {@link io.github.aandreakis.dblog.adapter.api.WatermarkWindowRuntime}.
 */
package io.github.aandreakis.dblog.adapter.api;
