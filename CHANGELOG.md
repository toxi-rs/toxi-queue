# Changelog — `toxi-queue`

Per-crate history extracted from the monolith changelog
([meshackbahati/toxi](https://github.com/meshackbahati/toxi/blob/main/CHANGELOG.md)),
which remains the full documentation hub.

## Unreleased

- **toxi-queue** (`3.1.1`): in-memory backend uses priority and delay
  heaps (O(log n)) instead of scans with memmoves (O(n)); worker
  diagnostics go through the `log` facade.
