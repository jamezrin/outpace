# Protected descriptor refusal (#279)

Status: implemented and independently reviewed; offline gates pass. PR integration and CI remain pending.

Recognized wrapped BT descriptors use root `premium`; ordinary and disk-enveloped metainfo use `info/premium`. Missing/integer zero means public; integer one means protected. Other values are invalid supported-format metadata. This is a deliberately strict Outpace policy, not a claim that every original-engine value is validated identically. Transport-envelope encryption and source signatures are separate concerns.

One wire classifier supplies protection state without changing encoded identity, geometry, authentication or public playback. Carry inspected public/protected distinctly from absent descriptor state through resolution, selection and caches. Protected and malformed-protection errors are typed terminal outcomes: catalog or URL fallback must never turn them into an uninspected selector. Playback rejects before tracker/DHT/peer/session work and before minting control/playback URLs, including warm-cache and existing-session paths.

Inspection-only analysis may report protected descriptors as `is_encrypted: 1` and public descriptors as zero. Bare-hash/magnet offline analysis returns the existing error envelope with `descriptor metadata is unavailable`, rather than claiming inspection. Existing allowed playback of uninspected selectors remains supported; inspection does not replace existing authorization checks. Native live, HLS, VOD, CLI and compatibility routes share admission. Protected getstream JSON returns response null and the stable `encrypted content is not supported` error, without URLs or token creation. Malformed protection uses a distinct stable error.

Validation uses synthetic values only: absent/zero/one/other integer/noninteger for each supported representation; authoritative root/nested placement; unchanged hashing; protected content-id/URL and cached admission; public controls; typed terminal fallback; uninspected analysis; no discovery/start/token side effects. No protected playback, decryption, accounts, new live observation, extra fetch, or full API parity is introduced.
