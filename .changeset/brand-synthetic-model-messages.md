---
"eve": patch
---

Brand framework-authored user-role messages in `gen_ai.input.messages` with namespaced semantic provenance: `context.instruction`, `context.state`, `context.compaction`, `memory.load`, `execution.background_task`, `execution.continuation`, or `execution.retry`. Model hooks and adapters can now distinguish them from real user input.
