# Phase 8 Fault Matrix

The named tests are executable evidence. Several rows share one adversarial trace where that
trace establishes the same ordering property without multiplying ceremonial test cases.

| # | Requirement | Evidence |
|---:|---|---|
| 1 | Conditional bootstrap | `test_bootstrap_is_conditional_and_runtime_names_do_not_reuse_sessions` |
| 2 | Competing owners/monotonic epochs | `test_multiprocess_takeovers_serialize_distinct_epochs` |
| 3 | Runtime name does not reuse session | bootstrap/session test above; CLI generates UUID sessions |
| 4 | Pause-after-check fence | `test_pause_after_owner_check_then_takeover_rejects_old_commit` |
| 5 | Late old upload is harmless | stale-owner demos, both modes |
| 6 | Old descriptor unreachable | pause race plus accepted-chain reachability assertions |
| 7 | Commit wins before takeover | `test_commit_before_takeover_is_preserved_and_retry_resolves` |
| 8 | Takeover wins before commit | pause race and stale-owner demos |
| 9 | Stable immutable retry | `test_s3_authority_classifies_response_loss_unknown_and_immutable_retry` |
| 10 | CAS response loss | `test_successful_cas_response_loss_resolves_from_history` |
| 11 | Nonexecuted CAS not guessed | coordinator leaves unchanged-revision UNKNOWN blocked |
| 12 | Delayed old revision cannot revive | pause race |
| 13 | Accepted/local-failed blocks | `test_accepted_local_apply_failure_blocks_continuation` |
| 14 | Retry identity across takeover | integrated unsealed recovery test and ACK-loss demo |
| 15 | Shared batch split decision | `test_shared_batch_allows_one_tenant_acceptance_and_one_fence` |
| 16 | Conflicting orphan ignored | stale-owner demos accept successor at the orphan's sequence |
| 17 | Conflicting accepted mapping fails | immutable/descriptor integrity and single CAS lineage |
| 18 | Missing/corrupt committed object | `test_missing_or_corrupt_required_descriptor_fails_closed` |
| 19 | Wrong identity/cycle/budget | strict model decoders and bounded `accepted_history` traversal |
| 20 | Authority outage blocks | fenced engine maps authority failure to non-serving outcome |
| 21 | Stale reads fenced | `test_integrated_engine_fences_reads_and_recovers_without_seal` |
| 22 | Ownership loss before activation | `test_recovery_losing_ownership_before_activation_preserves_destination` |
| 23 | Successor killed; later owner completes | `test_successor_process_killed_during_recovery_then_later_owner_completes` |
| 24 | Stale checkpoint registration | checkpoint registration test |
| 25 | Older valid checkpoint prefix | checkpoint registration test commits through 2, registers 1 |
| 26 | Interrupted restore isolation | activation-loss test preserves destination and removes staging |
| 27 | Worker/queue/shutdown | retained Phase 6/7 batching tests plus shared-batch test |
| 28 | Metrics cannot alter decision | retained `test_metrics_failure_cannot_change_commit` |
| 29 | Deadline is not cancellation | UNKNOWN state table and S3 lost-response test |
| 30 | Real kill/cache deletion/no seal | cache-loss demos, single and batch |
| 31 | Correct next sequence | cache-loss demos report sequence 2 |
| 32 | Protocol compatibility | full legacy suite; fresh fenced namespace required |
| 33 | Request accounting | persistent SQLite counters and S3 adapter counters in artifacts |
| 34 | Failure artifacts | retained JSON evidence includes events, outcomes, counters, and cleanup status |

The bounded independent model is `test_exhaustive_two_owner_takeover_model`; it explores 30
valid event orderings without calling production protocol methods.
