# Guarded DHT workers implementation plan

> Use superpowers:executing-plans with test-first contracts; user waived routine approvals.

**Goal:** Fixed default-deny real-worker input/result boundaries, offlineonly.
**Architecture:** Standalone worker modules and binary, custom cfg(test) entry, shared Python wire validator/conformance vectors. Parent existing PIDnamespace owner bounds lifecycle.
**Tech:** pinnedRust/Tokio; standalone optional libc/serde_json1.0.151; root existing dev primitives only.
**Spec:** docs/superpowers/specs/2026-11-05-dht-offline-workers-44-design.md

## Current status

Implementation, offline gates and independent static review complete; PR integration pending. Same-entry real target positives cover each gate/adapter/encoder, with separate compiled executable denials. Positive binary-main OS-environment/stdout integration is not established. Parent fixed-launch composition has fake-only coverage. #44 remains open for public destination/live policy and comparison.

## Constraints/review focus

No productionAPI/rootmanifest/lock changes; no publicmode/liveprobe; no active195closuremutations. Namespaceactualnetns/alltables/bothfamilies, boundedinspectorcapture/cleanup. Denialbeforesecretruntimeadapter. No unknown/duplicate/nonfinite/oversize records accepted. Synthetic input and separatepositive-exec limit explicit. Existing15sW0 includesconstructor; parent30s envelope useshelper≤27.8+cleanupreserve, notselfpreemptionclaim.

## Task1: Request and record boundary

Files tools/dhtcompare/src/worker/{mod,input,record}.rs, src/lib.rs, Cargo.toml/Cargo.lock, tests/worker_boundary.rs; worker-contract.md; worker_records.py/test_worker_records.py/vectors.
Interfaces: Role(Mainline,N0), parse_role_args(IntoIterator<OsString>)->Result<Role,WorkerError>; parse_environment(Role,IntoIterator<(OsString,OsString)>)->Result<Request,WorkerError>; encode_record(Role,&candidates::Record)->Result<Vec<u8>,WorkerError>. Requestprivateinput, redactedDebug. Python validate_record(payload:bytes,role:str)->dict and extract_record(role,result,exact_custom_test=None)->dict withstrictschema/process/framing.
- [x] RED tests: exactargs/noextras/nonUnicode; fourenvfields/mode/role match; missing/duplicate/unknownprefix/invalidtarget/seeds/zeroport/nonloopback; Debug/errorredaction. Record exactversion/keys, counts/timing/overshoot, boundedencoding; Python duplicatekeys/nonfinite/booleans/deepJSON/extra-record/truncation/cleanup and role mismatch.
- [x] Implement minimal parsing/encoding/sharedwirecontract; optional workersfeature includescandidates/libc/serde_json1.0.151, requiredfeaturebinarylater. Rootdependenciesunchanged.
- [x] GREEN focused contract tests and noidentifier/outputleak assertions.

## Task2: Actual namespace gate and upstream entry

Files worker/isolation.rs, worker/mod.rs, src/bin/dht-worker.rs and tests/worker_entry.rs. NamespaceError static. attest_offline()->Result<(),NamespaceError> readsPID1 then boundedactualipJSON. snapshot validator pure; capture hasone2sdeadline/16KiBpercommand, nonblockingbothpipes, kill/reap.
Interface pubentry(args,environmentclosure,outwriter)->i32; onlybinary andtests use; actualgate neverinjected. Fixedenvclosure allowsdelayreading untilgate. No caller-selectedadapterfactory/timebudget.
- [x] RED puretopologyvectors: extra/down/nonloopbacklink/address, IPv4+IPv6external/local/default/gateway, malformed/oversize/failinginspector andtimeoutcleanup. Denial-order tests callback mustnotrun onbadargs; namespacefailurebeforeenv.
- [x] Implement gate and synchronousentry; createcurrentthreadruntime onlyaftervalidrequest, runactualcandidate andemitoneboundedline. Binary invaliddefault/args exits64; validunconfinedentry65.
- [x] Confinedignoredsame-entrymainline/n0target positives: realseed/exactquery, actualgate/adapter/encoder, ownedserverjoin beforeassert, commonrecordvalidator. Realexecnegative cases outsideandinsideisolation reachbothgates; nofalsepositivepositive-execclaim.

## Task3: Custom comparison worker

Files crates/ace-swarm/src/dht/comparison/worker.rs and comparison.rs childmodulecfgtest/Linux. Usesexistingserde_json/libc andrun_comparison. Localrequest/gate mirrorscontract, no outside-package reads orproductionAPI.
- [x] RED localrequest/topology/recordcontrols matchingcontract, then synchronousignoredcustom_offline_worker fixedrolecustom. Validatebeforeworkercontrolledruntime; emitandflushoneDHT_COMPARE_RECORD JSONline. Errorsstatic/rawvaluesneverformatted.
- [x] Confinedsame-entryactualwalkertarget positive and compiledtestentrydenials, exactoneignoredtest/wirewitness/knownnamespacecleanup. Rootproductiontests/pathsunchanged.

## Task4: Fixed qualification and integration

Files tools/dhtcompare worker qualification helper/tests/README, old44planstatus.
- [x] Parentbuildscleanenvprefix, pinsactualbinaries, fixedmodes/roles/paths andoneabsolute30sperworkerenvelope, refusewithoutreserve; capture/privateprefix/drain/exit/cleanupusingexistingowner. No publicinputnetworkmode.
- [x] Sharedvalidator checksactualemittedbytes fromall3same-entry positives and actualcompiledexecdenials (missing/badmode/wrongrole/nonloopback), exacttestcount forcustom. Cappedsecretleak audit ofstdout/stderr; no rawregistry/hash persistence.
- [x] Focused root/tooltests, allfeaturesClippy, fmt/hygiene/diff, explicitnamespacequalification. Recordbinaries/sources/limits and rootlockequality.
- [ ] Independentwholeincrementreview, fixconcretefindings, normalhookcommit/PR/CI/exactheadmerge. Keep44OPEN forpublicdestination/server/actorpolicy andlivecomparison/recommendation.

Self-review: interfaces/dataownership/lifecycle are explicit; no missing userdesignchoice. Rootimplementsinline, researchagents own separateprivatereports only. Do notexpandpublicegressscope to make offlineworker looklikecompletedmilestone.
