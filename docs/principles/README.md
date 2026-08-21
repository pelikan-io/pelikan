# README: How These Principles Were Produced

This file documents the method behind [`PRINCIPLES.md`](PRINCIPLES.md), so the process can be repeated — with other experts, other domains, or agents on either side of the table. The companion document captures *what* was extracted; this one captures *how*, because the project's goal is principles that are actually applicable, and applicability turned out to depend as much on the extraction method as on the expert's knowledge.

## The core stance

**Target contested convictions, not consensus.** The opening question asked for a belief that a meaningful fraction of competent engineers would dispute, because that is where earned rationale lives. Consensus items ("test your code") can be filled in later or skipped; they carry no information about judgment.

**The unit of extraction is principle → rationale → concrete example → boundary.** A principle stated without its mechanism is unfalsifiable; without an example it is unusable; without its boundary it is a platitude. The boundary question — "where does this break, and what do you pay to cross it?" — was asked of essentially every principle, and the exceptions became part of the principles themselves (uniform load shedding has named exception conditions; partition-over-sharing has a documented counterexample from the expert's own system).

**Mechanism over outcome.** Whenever a claim was causal ("retries can take the system down"), the interviewer demanded the full chain (trigger → timeout → retry → what makes it self-sustaining) and, where several candidate mechanisms could explain the same claim, enumerated them and asked which one was actually doing the work. Getting the mechanism right matters because the mechanism, not the slogan, determines when the principle applies.

## The interview protocol

**One question at a time**, with a short look-ahead summary of queued threads at the top when useful. This was an explicit correction from the expert early on and it held for the rest of the interview. Multi-part questions lose answers; a visible queue loses nothing.

**Decompose bundled claims before digging.** The opening thesis contained three distinct claims (a behavioral one, an architectural one, a hardware-trend one); pulling them apart before generalizing prevented the weakest from hiding behind the strongest.

**Reflect back, then let the expert rule.** After each answer the interviewer restated the position precisely — often more explicitly than it was given — and the expert confirmed, extended, or corrected. The corrections were the single most valuable output of the whole process: logical versus runtime separation (dissolving a false performance-vs-modularity tradeoff), effort pooling driven by interestingness rather than risk-avoidance, default-to-visibility for adversary-prone logging (the interviewer's proposed rule ran the opposite way), and warmup as an operations rate limiter rather than a headroom term. A reflection the expert merely tolerates is worth little; a reflection precise enough to be *wrong* in a specific way is what surfaces the real position.

**Let the expert steer by contribution.** Repeatedly, the expert answered a different question than the one asked — adding a refinement, an exemplar, or a new axis instead. The method treats this as signal, not evasion: the volunteered material was folded in, the unanswered question went back in the queue, and questions that were sidestepped twice were usually being answered implicitly by the additions. Several of the strongest principles (the token-bucket coordinates, maintenance traffic as a fourth headroom term, the correlation → surprise → driver method) arrived this way.

**Name the interviewer's own framing as a candidate, not a fact.** Where the interviewer proposed a synthesis the expert had not stated (e.g., perturbation response as a below-the-knee leading indicator), it was offered explicitly for confirmation and, when no ruling came, recorded in the document as unconfirmed interviewer synthesis. Provenance discipline is what lets a reader trust the rest.

## Grounding in artifacts and reality

**Ingest the expert's prior artifacts, then ask for what they cannot carry.** Slide decks from two prior talks were downloaded and read mid-interview. The explicit move was: state what the slides establish, then identify what slides structurally omit — the dynamics between bullets, the why-it-doesn't-recover, the seduction behind a failure — and ask for exactly that. Interviewing someone *about* material you have not read produces flattery; reading it first produces the missing half.

**When a principle is mechanically checkable, check it.** The claim "every error-handling branch should emit a log line" was tested during the interview by a static audit of the actual open-source codebase (grep-level, proximity-based, limits acknowledged). The principle failed as literally stated — and the failure decomposed into legitimate categories (propagation, metrics-instead-of-logs, semantically-normal errors) plus genuine gaps, yielding a refined, CI-checkable form: *every error branch must terminate in a signal on some pillar; swallowing is the sin.* A principle that has survived contact with real code is categorically more trustworthy than one that has only survived conversation. The audit findings are recorded in the document as measurements, not recollections.

## Consolidation as part of the interview, not after it

**Write the document early and revise it live.** The first consolidated draft was produced roughly halfway through, explicitly so the expert could inspect the extraction for fidelity and gaps before going further — bones first, then meat, applied to the writing itself. Every subsequent session-segment ended with the new material folded in and the file re-presented.

**Maintain a gap ledger.** An explicit list of unopened topics was kept in the document and updated as items were covered, retired, or — importantly — *declined with a recorded reason* ("hard to generalize" is a principled exclusion, and writing it down is different from leaving a topic looking unfinished).

**Let the scope be corrected by the material.** The document began as "load-bearing systems" and was rescoped to "distributed real-time data services" when the expert judged that the principles cohere holistically only for that archetype. The claimed scope is itself a claim, subject to the same honesty as any principle — and the reclassification turned one apparent gap (correctness/consistency) into an out-of-scope-by-design note.

**Treat convergence as validation.** The strongest evidence that the extraction found real structure rather than a list: independent questions kept collapsing into the same answers. "Who sets headroom," "who picks KPIs," and "what tier for adversary-prone logs" all resolved to the same designer-builds-legibility / operator-owns-context contract; "when is adaptation safe" resolved to auditability three separate times; "increment under control" appeared as one protocol wearing four uniforms. When pressure from different directions produces the same load path, the structure is probably real.

## A note on reflexivity

The method ended up obeying the principles it was extracting: the document was built skeleton-first and iterated (P20–P24); every claim was pushed toward its seam rather than tested only at its comfortable operating point (P31–P33); the interview produced auditable artifacts — the document, this README, a provenance trail — rather than tacit understanding (P18); and nothing in it was sacred, including its own title (P25's closing law). That is probably not a coincidence: a method for extracting engineering judgment is itself a system under iteration, and the same physics apply.

## Reusing the method

To run this with another expert: read their artifacts first; open with the contested-conviction question; hold to one question at a time with a visible queue; reflect back precisely enough to be correctably wrong; chase every principle to mechanism, example, and boundary; verify anything checkable against real systems mid-stream; consolidate early into a document the expert reviews for fidelity; keep a gap ledger with reasons; mark synthesis as synthesis; and let the expert's corrections — of the content, the protocol, and the scope — outrank the plan.
