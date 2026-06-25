# Studio Context Recovery — 2026-06-24

## Product goal

Studio Agent is the primary product: a premium, agent-first content production system that turns a conversation into a controlled production manifest, approved assets, rendered scenes, QA, and a final upload package. The target is high-quality automated video production at a cost low enough to sell profitably.

The intended user flow is:

1. Open Studio directly into the Grok-style Studio Agent UI.
2. Select or infer the channel and load its durable memory.
3. Develop the idea and script conversationally.
4. Create a production/asset plan.
5. Approve stills and assets before paid image-to-video work.
6. Edit or regenerate individual scenes without rebuilding the full production.
7. Animate only approved scenes.
8. Assemble, QA, package, and deliver the final video.

Catalyst learning, channel memory, analytics, and background intelligence remain available without making the primary UI complicated.

## Recovered original thread

- Title: `Analyze Studio Agent`
- Thread ID: `019e9f40-9535-76a2-aefe-4abade5809e2`
- Raw session:
  `C:\Users\casey\.codex\sessions\2026\06\06\rollout-2026-06-06T19-24-35-019e9f40-9535-76a2-aefe-4abade5809e2.jsonl`
- The currently readable compressed JSONL was about 150 MB with 21,450 records and 201 user messages.
- The original accumulated context footprint was over 15 GB before compression, per the user. Do not describe 150 MB as the total historical context size.
- The Codex app reports the thread as `systemError`, but the raw session is intact.

The final usable state from that thread was:

- Agent-first UI work existed locally but had not been pushed.
- Studio Agent tool grounding and MrSkeleWelly category routing had already been pushed in commits `6d95b4fa` and `2f9fc794`.
- The remaining local diff accidentally removed screenshot paste, stream recovery, scene approval UI, and newer channel-grounding code.
- Deployment was intentionally stopped until those regressions were corrected.

## Recovery performed

The following old overlays were preserved in a named git stash and removed from the active working tree:

`stash@{0}: preserve-old-studio-overlays-before-context-recovery-2026-06-24`

Files preserved there:

- `studio_agent/runner.py`
- `ViralShorts-App/src/studio/components/agent/AgentJobDeliverable.tsx`
- `ViralShorts-App/src/studio/lib/streamAgentChat.ts`

Those overlays deleted newer committed reliability and approval behavior. The active working tree now uses the proven committed versions.

`AgentPanel.tsx` was repaired to retain:

- screenshot paste from clipboard items and files
- attachment image previews
- network-friendly error messages
- server-side stream recovery
- scene edit/regenerate reply presets
- live scene approval snapshot updates

After repair, `AgentPanel.tsx` matches the committed reliable implementation.

## Current uncommitted work

The remaining dirty state is mostly additive and falls into these groups:

### Agent-first navigation and frontend routing

- Studio entry/navigation changes in layout, dashboard, and shared API routing.
- Direct production backend routing to Fly.
- Stale checklist/leaderboard routing cleanup.

### Production control and queue reliability

- Lease-based Studio Agent queue admissions.
- Queue lane, priority, stage-gate, approval, and durable-state metadata.
- Read-only `/api/studio-agent/production-control`.
- Production-control metadata attached to job snapshots.
- Focused queue/control/router tests.

### Direct Anthropic model path

- Direct Anthropic as a first-class provider.
- Claude model alias normalization.
- Prompt/tool compaction and fallback handling.
- Provider/model-aware billing metadata.

### FAL and skeleton production reliability

- Shared FAL credential resolver accepting supported key aliases.
- Wardrobe continuity lock for skeleton image-to-video prompts.
- Stronger white-shirt/undershirt continuity guards.
- No paid generation was run during this recovery.

## Verification on 2026-06-24

Passed:

- `python -m unittest test_studio_agent_queue.py test_studio_agent_production_budget_control.py test_studio_agent_router_control.py`
- `python test_studio_agent_anti_hallucination.py`
- `python test_studio_agent_channel_guard.py`
- `python test_studio_agent_production_budget.py`
- `python test_studio_agent_render_qa.py`
- Python compilation for the touched Studio Agent and skeleton modules
- `npm.cmd run build` in `ViralShorts-App`
- `git diff --check`

No deployment, push, paid FAL generation, or destructive cleanup was performed during recovery.

## Next execution order

1. Review the remaining dirty changes by group and remove any additional stale overlays.
2. Add/confirm regression coverage for direct Anthropic billing and FAL credential aliases.
3. Verify the agent-first route in a local browser.
4. Run a no-cost end-to-end Studio Agent smoke test through chat, channel grounding, plan, and approval state.
5. Run one tightly budgeted paid short only after the no-cost path passes.
6. Commit in separate logical commits:
   - production control/queue
   - Anthropic/FAL reliability
   - skeleton wardrobe continuity
   - agent-first frontend/routing
7. Push, deploy backend/frontend, and verify production behavior before declaring launch readiness.

## Non-negotiable product rules

- Never claim a tool ran without a recorded backend tool result.
- Never claim a render or re-edit completed without a completed production result.
- Never mix channel memory, branding, analytics, or copyright packages.
- Never animate before still approval unless the user explicitly enables auto-approval.
- Preserve jobs, chats, approvals, and recovery state across browser/network failures.
- Prefer targeted scene edits over full regeneration.
- Keep paid generation behind explicit budgets and approval gates.
