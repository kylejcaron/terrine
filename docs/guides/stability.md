# API stability

Terrine is in alpha. Pre-releases (`0.1.0a1` and later alphas, betas and release
candidates before a final `0.Y.0`) may change anything, with a changelog entry.
From the first final `0.Y.0` release, minor releases may break the API with a
changelog entry; patch releases do not. From `1.0`, the stable API follows
[Semantic Versioning](https://semver.org/): breaking changes happen only in a
major release. Every module and name is stable, provisional or internal; which
backends verify an operation is a separate question, answered by the capability
matrix.

## Stability tiers

### Stable

From 1.0, breaking changes happen only in a major release.

- The `terrine` root facade and `terrine.recipes`.
- `terrine.metrics` and the constructors exported from it.
- `terrine.spec`, except provisional transforms and members listed below.
- `terrine.windows`, including `Window`, `Duration` and `Months`.
- `terrine.errors` and refusal codes raised by stable names.
- `terrine.dataset`, `terrine.model` (including `Catalog` and `Readiness`, except
  `Model.from_increment`) and `terrine.frame`, except provisional members listed
  below.
- The input declarations and metric types exported by `terrine.data`, including
  `EventMetric` and `Ratio`; returned metadata records are provisional.
- `terrine.reasons`: the missing-value reason vocabulary.
- The Design, dataset definition, Catalog, manifest and Arrow/Parquet metadata
  formats (see [saved artifacts](#saved-artifacts)).
- Frame output column names, types and roles for each shape.

Stability follows exports, not module names: a stable name is one listed in a
stable facade's `__all__`, together with its stable members (methods, attributes
and parameters). Import it from that facade. The defining module behind a facade,
such as `terrine.spec.expressions`, is internal. Provisional members of stable
classes are excluded from the stable surface.

### Provisional

A provisional name may change in a minor release, and every such change has a
changelog entry:

- `terrine.runtime`, `terrine.execution`, `terrine.agent`,
  `terrine.orchestration`, `terrine.adapters`, `terrine.export`,
  `terrine.simulate` and the `terrine` CLI;
- `Dataset`, `Frame.stage`, `Frame.expr`, and the result-metadata records
  exported by `terrine.data`;
- `Sequential`, `EventPanel`, `CalendarPanel`, `CalendarComparisonPanel`,
  `matched_t0` and `CalendarComparisonSpec`;
- `windows.duration`, `approx_distinct`, `approx_quantile` and
  `Model.from_increment`;
- `VerifiedInputBinding`, `ChangeSet`, `RefreshPlan`, `DatasetManifest`, lower-level
  data, filter and JSON aliases, and concrete expression result records when
  they appear in stable signatures. These types describe variants and returned
  values, not additional stable construction inputs.
- refusal context fields such as `reason` and `field`. Refusal codes are frozen
  only when raised by a stable name.

Their docstrings say they are provisional, and the
[focused facades](public-api.md#focused-facades) table lists each facade's tier.

### Internal

Every other module, whatever its name, is internal and may change in any
release: for example `terrine.compile`, `terrine.backends`, `terrine.semantic`,
`terrine.temporal` and the defining modules behind stable facades.

## Backend verification

A tier says whether a name may change, not where it runs. Which engines verify
each operation is recorded in the per-operation
[backend compatibility matrix](backend-compatibility.md#operation-evidence) and
summarized in [dataset capabilities](capabilities.md); an engine that does not
verify an operation refuses it with a coded refusal. Numerical results follow
the [shared numerical contract](backend-compatibility.md#shared-numerical-contract):
native and exact float results meet their stated bounds, not exact bits.

## Deprecation

After 1.0, a stable name is removed only in a major release, and only after at
least one minor release in which using it emits `DeprecationWarning`.

## Signature check

Before `1.0.0rc1`, the newest final `v0.*` tag is the cross-minor informational
baseline; development, pre-release and post-release tags are excluded. When a
final tag exists in the current 0.x minor, that tag is the enforced baseline,
and the newest cross-minor tag is also reported informationally. Without a
same-minor tag, cross-minor breakages are reported but permitted. From
`1.0.0rc1`, the newest `v1.*` tag, including release candidates, is enforced.
Pass `--against` to compare with a specific tag:
```bash
uv run python scripts/api_check.py
uv run python scripts/api_check.py --against v1.2.0
```

Stable-surface snapshots live at `tests/artifacts/surface/<version>.txt`. Create
one for every `v0.Y.0`, release-candidate and final release tag with
`uv run python scripts/api_check.py --snapshot <path>`. Each sorted line records
a stable callable's qualified name and signature. CI compares the working
surface with the newest snapshot by version order; an absent snapshot is
reported and skipped until the first release snapshot is added. During 0.x,
differences are printed for review and recorded as changelog additions or
breaking changes. From `1.0.0rc1`, differences fail CI unless the snapshot is
updated in the same change. `--enforce` enables this failure manually before
that version.

It resolves the stable surface from exports: every name in a stable facade's
`__all__`, followed through re-exports to its definition, with that definition's
members. Provisional names are excluded after that resolution, including a
provisional method of a stable class such as `Model.from_increment`. It reports
stable names that leave a facade's `__all__` and signature changes such as a
removed, reordered or newly required parameter.

Each stable callable's parameter and return annotations are checked as well: any
Terrine type must be exported from a stable facade or classified as a
provisional variant. An unclassified type fails the check.

Catalog's nested records are governed by the Catalog document version, not by
the Python signature check. The check therefore exempts only generated
`__init__` methods of Pydantic `BaseModel` subclasses; handwritten constructors
remain subject to the annotation rule above.

| Exit status | Meaning |
| --- | --- |
| 0 | Compatible, no eligible baseline exists, or breakages are permitted across 0.x minor releases |
| 1 | A stable name disappeared or its signature broke within the same compatibility line, a current stable export does not resolve to a definition the check can read, or a stable signature names an unclassified Terrine type |
| 2 | Comparison history unavailable: the tag cannot be loaded, or a shallow clone cannot show which tags exist |

The check reads source without importing it, so a name a facade provides only
through a module `__getattr__` also needs an import it can read, such as the
root's `TYPE_CHECKING` imports.

The `api-compat` CI job runs the check on every push with full history and
tags, and `ci-ok` requires it. It covers signatures only: formats and outputs
are covered by the saved-artifact checks below, and numerics by the
conformance known answers.

## Saved artifacts

From `0.1.0a1`:

| Artifact | Promise |
| --- | --- |
| Design and definition JSON/YAML, Catalog YAML | Every 1.x release reads every 1.x document version |
| Manifests, Arrow/Parquet metadata, and durable reopen records: `ReopenableSnapshot` and the snapshot references a published dataset needs to reopen | Readable throughout 1.x |
| Runtime owner database | A recorded schema version, currently 1. A namespace from before 1.0, or from a newer release, refuses with `terrine.runtime.schema_unsupported` before any write; a later version migrates through the owner (see [runtime schema](materialization-runtime.md#build-and-plan-from-a-model-and-design)) |
| Ephemeral execution records: saved plans, run requests, plan invocations and run receipts | Same format version only; another version refuses with its existing code, such as `terrine.runtime.saved_plan_mismatch`, and is planned again from the Model and Design |

Design and dataset-definition documents are version 4, with fields equal to
their defaults omitted ([design specifications](design-specs.md)); Catalog
documents are version 3 and version 2 Catalog documents still load with every
measure exact; manifest and saved-plan envelopes are version 1.

### Identity rules

- A stored record's identity is the digest recorded with it. Reading verifies
  that digest by hashing the stored JSON object under fixed canonical JSON rules
  (sorted keys, compact separators, UTF-8, no NaN) before decoding, never by
  re-encoding the decoded object. Runtime lookups of stored artifacts use the
  recorded digest.
- A decoded `DatasetManifest` keeps its recorded definition digest. Reopening a
  published dataset decodes the stored publication manifest and compares it
  with the manifest being opened as objects, ignoring only the envelope's
  definition reference, never as re-encoded text. A manifest edited with
  `dataclasses.replace` therefore cannot authenticate as the original
  publication and refuses with `terrine.dataset.invalid_generation`. A
  reference envelope resolves its definition through `DatasetDefinitionStore`,
  which verifies the stored bytes.
- Saved plans verify their recorded Design and definition digests over the
  stored JSON payloads. Agent dataset references (`DatasetTools`) are created
  from and resolved against recorded digests and the publication text read from
  the owner database, never the text of an in-memory object.
- A decoded object computes its own digest from its canonical encoding, so an
  edited object, for example through `dataclasses.replace`, has a new identity.
- Canonical documents omit fields equal to their defaults, so a later optional
  field does not change existing identities. A default never changes within a
  document version; changing one requires a new version whose decoder supplies
  the old version's default.
- The Catalog has one canonical payload, with defaults omitted, used for Catalog
  documents and for every runtime identity derived from a Catalog: the model
  hash and `ModelReference`. A Catalog that gains a defaulted field therefore
  keeps its model hash, saved plans and agent references. Measures omit `exact`
  when exact, and the identity payload declares version 2 unless a measure is
  native, so a version 2 Catalog keeps its identities; documents are written as
  version 3.

### How the promises are checked

- `tests/artifacts/<generation>/` keeps one set of path-independent documents
  for each format generation since `0.1.0a1`, written by
  `scripts/write_artifact_corpus.py`: Design, definition and Catalog documents,
  a manifest, Arrow/Parquet metadata, a receipt and the expected output schema
  and roles of each stored Design. A [unit test](../../tests/test_artifact_corpus.py)
  decodes every document and verifies its recorded digests and checksums; an
  [integration test](../../integration/test_artifact_corpus_schema.py) compiles
  each stored Design with the current code and requires the recorded schema and
  roles.
- Release tags in the 0.x series require a successful CI run. Release candidates
  and final tags require successful CI, PostgreSQL, and scale jobs; the scale
  tier includes the release-upgrade test. BigQuery and Snowflake are advisory
  in the automated gate, but each needs a green run within seven days of the
  tag or a waiver recorded in the release notes. Record each cloud backend's
  latest passing release as `verified at vX.Y` in the [backend compatibility
  table](backend-compatibility.md#operation-regression-environment).
- An [upgrade test](../../integration/test_release_upgrade.py) installs each
  published release listed in `tests/artifacts/RELEASES`, starting with
  `0.1.0a1`, into an isolated interpreter, stages a dataset with it and reopens
  that dataset in place with the current code. It runs in the weekly scale tier.
- Adding a format generation, and adding a release to `RELEASES`, are release
  checklist steps.
