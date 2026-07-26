# Convey artifact roles through logical paths

A snapshot's artifacts carry no role or kind field. What an artifact means within a snapshot —
transcript, summary, extracted tool output — is conveyed by its logical path under a reserved naming
convention that the manifest's `artifact_set_version` versions. The manifest schema keeps rejecting
unknown fields, and the server never branches on logical paths; role interpretation belongs entirely
to clients and consumers reading the convention for the artifact-set version they understand.

This keeps the archive contract stable while artifact sets evolve: adding a new role is a client-side
convention change under a bumped artifact-set version, not a server migration. A structured role field
would either be free-form provenance the server must store but ignore, or an enum the server must
maintain in lockstep with every source adapter — both pull content semantics toward Patwari, which
ADR 0001 keeps out of this bounded context.
