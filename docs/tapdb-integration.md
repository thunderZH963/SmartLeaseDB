# Database adapter integration

The runtime builds independently of TapDB. A native adapter must connect the
following interfaces to actual operator execution and allocation lifetimes.
No native SQL-engine adapter is included in this repository.

## Work boundaries and state

- Define stable query, snapshot, partition, and morsel identifiers.
- Check execution authority between bounded input batches.
- Export logical state and the matching input cursor at a safe boundary.
- For aggregation, include partial groups and pending updates.
- For joins, preserve duplicate matches and compatible build/probe partitions.
- Reject state exceeding the admitted checkpoint envelope; use committed-boundary
  replay or compute execution when continuation is unsupported.

## Output commitment

Stage output privately. Publish it only through a recoverable coordinator record
that checks the placement generation and atomically records completion.
Consumers must not see uncommitted output. Retries must return the recorded
commit decision rather than publish a second result.

## Resource integration

Connect real engine buffers to tracked allocation lifetimes. Operator teardown
alone does not prove that queued messages or asynchronous I/O released references.
Retain memory and transport charges until their final completion callbacks.
Use the enforcer release receipt to retire the corresponding broker reservation.

## Validation

Compare every result with uninterrupted execution. Inject recall at each safe
boundary, retain an output buffer past operator completion, duplicate release
receipts, and restart the coordinator after commitment. Confirm that old attempts
cannot publish output and unreleased resources remain charged.

See [execution](execution.md) and [enforcer](enforcer.md) for API semantics.
