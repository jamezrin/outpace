# Whole live-piece publication

Incoming live peer chunks are assembled before reseed publication. Newly completed
pieces expose their full wire bytes, including the signature trailer, before the
ordered playback drain strips that trailer. A completion ahead of the playback
cursor can therefore be retained immediately.

Configured source authentication must succeed. Zero-trailer pieces without a
verifier are explicitly unsigned assembly acceptance; a nonzero trailer without
a usable key remains playback-compatible but is not published. Header conflicts
or unsupported publication geometry disable publication without adding playback
rejections or changing contribution accounting. Completed duplicates are no-ops.

The whole-piece store interface trusts its caller. It validates exact geometry
and publishes bytes, header and availability together. Existing retained complete
pieces win duplicate publication unchanged. Memory insertion is atomic under the
store mutex. Disk publication writes an unpublished temporary file, installs it,
and commits the complete index and budget under the actor's metadata lock; file
operations occur outside that lock. This is process-level atomicity, not crash
recovery or power-loss durability.

One disk publication permit is acquired before copying the wire bytes and remains
owned by the command through success or failure. Cancellation before admission
publishes nothing; after admission the actor may finish. Pending copied bytes are
accounted separately from retained cache bytes. Cache write failure does not
reject otherwise accepted playback. Trusted local broadcast chunk publication
and verified VOD chunk publication retain their existing contracts, including
broadcast cancellation fences and short final VOD pieces.

The issue #285 regression and contract tests are the intended evidence for the
#208 serving gate. This note records policy; it does not certify that the gate
has passed. Execution and independent review are required before publication.
