# Releasing the Go storage modules

The three modules remain independently consumable. Local development and CI use
`go.work` to select sibling source trees, with version-specific workspace
replacements so Go can load the graph before coordinated tags exist. Their `go.mod` files have normal
versioned requirements and no filesystem replacements. A dependency's replace
directives are not inherited by consumers, so the former `v0.0.0` plus local
replace setup could not be imported normally from another repository.

**v0.1.0 is prepared, not published by this change.** Until the tags below exist,
external `go get` and `make check-release` must fail. Passing `make check` in the
workspace is not evidence that these releases exist.

## First coordinated release

1. Merge this PR through the normal merge queue. Fetch `origin/master` over HTTPS
   and verify that it contains the reviewed changes. Use the final merged commit,
   not a feature-branch hash: merge queues may rewrite commit hashes.
2. As a maintainer permitted by the repository's tag rules, create these three
   annotated tags at that same verified merged commit, then push them together:

   - `golang/storage/providercontracts/v0.1.0`
   - `golang/storage/providers/aws/v0.1.0`
   - `golang/storage/providers/v0.1.0`

   The prefixes are required because each module lives in a repository subdirectory.
   Publishing all three completes the dependency graph. Never replace an existing
   released tag; if a release is wrong, prepare and publish a new version.
3. Run `make check-release` from the released checkout. It creates a temporary
   consumer outside the workspace, downloads the three normal module paths at
   v0.1.0, rejects replacements, and builds against the public query and loader
   APIs with `GOWORK=off`. It needs network access but no AWS credentials/resources.
   `RELEASE_VERSION` can select a later coordinated release. A proxy may take time
   to observe new tags; do not interpret that failure as a successful publication.
4. Only after that check passes, update downstream users such as the LLM worker.

Tag creation is a separate maintainer action; this PR adds no automated publishing
permissions or actions. It does not create tags or claim that the library is
already published. In particular, merging alone does not close this release gate.

## Subsequent changes

When a module adds APIs required by another module in this repository, bump the
consumer's requirement to the intended compatible release. Review and test all
modules together in the workspace, then publish the required modules from merged
master before upgrading external consumers. Modules without changes do not need
new versions. The `check-release` convenience target assumes a coordinated common
version; independently versioned releases need equivalent consumer checks with
those explicit module versions.

Keep `go.sum` files per module. Workspace checks do not validate standalone
manifests or prove release availability. An isolated local-proxy test can verify
the proposed module archives and dependency graph before merge, but must be
reported separately from real GitHub/proxy publication.

References:
- [Go module replacement rules](https://go.dev/ref/mod#go-mod-file-replace)
- [Managing module source and subdirectory tags](https://go.dev/doc/modules/managing-source)
- [Publishing a module](https://go.dev/doc/modules/publishing)
