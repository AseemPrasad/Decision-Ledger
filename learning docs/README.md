# Decision Ledger — Learning Documentation

A reverse-engineered, structured learning curriculum for the **Decision Ledger**
repository. The implementation is already complete; these documents exist to
teach you *what* the system does, *how* it flows, *why* it was built this way,
*what* the alternatives and tradeoffs are, and *what* you need to know to extend
or redesign it yourself.

## How this documentation is organized

| File | Covers | Best for |
| --- | --- | --- |
| `01-executive-summary-and-repository-map.md` | Executive summary, full repo inventory (Phase 1) | Orientation |
| `02-system-mental-model.md` | High-level mental model (Phase 2) | Orientation |
| `03-architecture-overview.md` | Architecture style, layers, component architecture (Phase 3) | Architecture study |
| `04-data-architecture.md` | Schema, indexes, join semantics, durability (Phase 3/6) | Data study |
| `05-runtime-flows.md` | Traced end-to-end operations (Phase 4) | Behavioral study |
| `06-features-and-design-patterns.md` | Feature catalog + software-engineering concepts (Phases 5/8) | Concept study |
| `07-architecture-decisions-and-tradeoffs.md` | Decision inventory + alternatives (Phase 6/11) | Architecture reasoning |
| `08-crosscutting-concerns.md` | Security, reliability, performance, testing, infrastructure | Operations study |
| `09-weaknesses-and-technical-debt.md` | Risks, smells, gaps (Phase 7) | Critical review |
| `10-learning-curriculum.md` | Beginner→Senior learning path (Phase 8) | Self-study |
| `11-knowledge-gaps-and-deep-dive-order.md` | Unverifiable unknowns + recommended reading order | Directed study |
| `12-architecture-diagrams.md` | 10 Mermaid architecture diagrams (context, containers, components, dependencies, request lifecycle, data flow, auth, ER, sequences, deployment) each with reading notes + self-test questions | Visual study |
| `13-all-features-deep-dive.md` | Complete functional deep-dive of every feature: overview, entry points, execution traces, data flow, architecture, design decisions, failure/security/performance/testing analysis, 3 alternative designs, 30 learning questions + answer key, implementation exercise | Master the loop |
| `14-architectural-reasoning.md` | Senior-architect review: 15 significant decisions each examined as WHY THIS / WHY NOT THAT / TRADEOFF / FAILURE POINT / CHANGE CONDITION / SCALE CONDITION / LEARNING QUESTION, with cross-references and an evidence map | Understand the reasoning |

## Conventions used throughout

* **Evidence labels.** Whenever a "why" is explained, it is tagged:
  * `[E]` — **Evidence**: directly observable from the code, configuration, or
    docs in the repository.
  * `[I]` — **Inference**: a reasonable engineering explanation consistent with
    the evidence, but not stated by the author.
  * `[A]` — **Assumption**: plausible but cannot be verified from the repository.
* File references use `path:line` so you can jump straight to the source.
* Code is **not** modified by these documents, and no requirements beyond the
  repository's own behavior are invented.

## Suggested starting points

* **15 minutes:** read `01` then `02`.
* **First working day:** `01`–`05`, then run the examples and tests:
  `python -m pytest`, `python src/examples/basic_serving.py`.
* **Architecture interview prep:** `03`, `07`, `09`, `11`.
* **Building something on top:** `04`, `05`, `06`, `10`.