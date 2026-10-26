# PES boundaries for packet-local access detection

## Intent and evidence

Investigation for #246 found that H.264/HEVC access heuristics scan the entire TS payload, whereas MPEG-2 already validates the video PES envelope and restricts inspection to elementary bytes. A NAL-looking pattern in opaque PES private data or after a declared PES end is not evidence of an access picture. Behavioral reproduction is required before production implementation. This does not establish the historical reconnect stall's cause.

## Chosen approach

Under the user's autonomous execution authorization, the builder selects a shared, allocation-free, packet-local video PES elementary-slice helper, reusing the existing MPEG-2 optional-header validation. The H.264/HEVC Annex-B scan and MPEG-2 first-picture scan consume only that slice. A complete PES/access-unit assembler would support more split headers but expands lifetime/state scope without evidence requiring it. Diagnostics alone would leave a demonstrated false positive intact.

## Contract

- Require a complete packet-local video PES prefix/header and existing optional-field syntax validation before no-RAI elementary inspection.
- A nonzero PES_packet_length bounds the inspected bytes; a zero length permits the remaining packet payload. No scanning of optional fields or bytes after the declared end.
- Incomplete/malformed/split PES headers remain conservative false negatives. No new cross-packet assembly.
- Preserve selected PID, PUSI, codec gating, existing NAL type policy, MPEG-2 opt-in and first-picture behavior, and the independent RAI path.
- Preserve gate/reset ordering, table caching, byte delivery after gate admission, and all scheduling, timeout, buffer and network defaults.
- No new dependencies, operational logs, exposed API or configuration. Library parsing remains silent.
- Synthetic data only in tracked files. Never include real identifiers, stream names, engine artifacts or private captures.

## Validation

Before implementation, valid H.264/HEVC PES private-field false access cases must fail for the predicted behavior; actual elementary IDR/IRAP controls must pass. Cover both VideoAccessPointState and KeyframeGate, explicit declared ends, complete no-optional and PTS-bearing real access, malformed optional syntax, split headers, wrong PID/codec/PUSI, and unchanged RAI. Existing invalid synthetic positive fixtures may be corrected to valid PES without weakening their assertions. Run focused media tests, workspace tests, Clippy warnings denied, formatting, hygiene and independent review; require CI before merge. Runtime conclusions remain separate.

## Approval

Routine bounded design selected by root under the user's explicit authorization to proceed through planning, implementation, review and merge without repeated approvals.
