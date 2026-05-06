# Security

This repository is a reference implementation published under the MIT License. It
is intentionally low-maintenance (see [CONTRIBUTING.md](CONTRIBUTING.md)), but
concrete, reproducible security defects within the documented scope are welcome as
private reports.

## Reporting a vulnerability

Please report privately via GitHub Security Advisories — open the repository's
**Security** tab and click **Report a vulnerability**. Include:

- a minimum reproduction (command, config, repo version, JDK, source database
  family and version)
- observed impact and attack prerequisites
- a suggested fix, if you have one

There is no SLA. Response is best-effort; there is no guarantee of fix, backport,
or coordinated disclosure.

## In scope

- remote code execution
- credential leakage in logs, errors, or crash artifacts beyond what is already
  documented
- SQL injection, path traversal, TLS / certificate-validation gaps
- denial of service against the DBLog process from crafted input (malformed
  source events, malformed control-plane requests, etc.)
- bypasses of documented fail-closed boundaries — watermark invariants, schema
  drift policy, metadata-row validation, single-owner claim

## Out of scope (documented by design)

The following are documented intentional limits of this reference implementation,
not vulnerabilities:

- **No control-plane authn/authz.** The HTTP control plane binds to `127.0.0.1`
  by default and has no built-in authentication. Exposing it beyond loopback is
  the operator's responsibility. See
  [docs/OPERATION.md § 5.1.1](docs/OPERATION.md).
- **Plaintext credentials in configuration.** Source and target database
  passwords in `application.properties` are plain strings; they will appear in
  heap and thread dumps. See [docs/OPERATION.md § 9.1.2](docs/OPERATION.md).
- **Disposable demo fixture credentials.** The Docker fixtures under
  `ops/docker/` use `dblog/dblog` and `postgres/postgres` and bind to
  `127.0.0.1` only. Do not run them in environments where those ports are
  reachable from an untrusted network.
- **Educational tap blocking the pump.** When `dblog.tap.enabled=true`, a slow
  subscriber intentionally stalls the DBLog pump. That is the feature, not a
  denial-of-service bug. See [docs/CONTROL_PLANE.md § 5.4](docs/CONTROL_PLANE.md).
- **Single-host, no HA, no fencing.** See
  [docs/OPERATION.md § 2.5](docs/OPERATION.md).
- **Third-party patent questions.** Not a security concern for this repository.
  See [PATENTS.md](PATENTS.md).

A report that restates one of the items above without a novel attack is not a
vulnerability and will be closed.
