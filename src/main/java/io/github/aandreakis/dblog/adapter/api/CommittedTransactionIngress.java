package io.github.aandreakis.dblog.adapter.api;

public interface CommittedTransactionIngress<TX extends SourceTransaction<?>> {
  void enqueueCommittedTransaction(TX transaction);
}
