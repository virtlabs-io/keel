#!/usr/bin/env bash
# scripts/make-release.sh — atomically bump the KEEL version, commit, and tag.
#
# Usage:
#   scripts/make-release.sh <version>
#
# <version> examples:
#   0.5.5          → tag v0.5.5   (no pre-release suffix)
#   0.5.5-alpha    → tag v0.5.5-alpha
#   v0.5.5-alpha   → same (leading 'v' is optional)
#
# What it does:
#   1.  Validates the version format.
#   2.  Checks there are no unstaged changes that would be missed.
#   3.  Updates  CMakeLists.txt   project(keel VERSION <MAJOR.MINOR.PATCH>)
#   4.  Updates  src/main/main.c  KEEL_VERSION_{MAJOR,MINOR,PATCH} fallback guards.
#   5.  Adds a [v<version>] CHANGELOG.md stub if the section is absent.
#   6.  Activates .githooks (sets core.hooksPath) so the pre-push hook fires.
#   7.  Commits ALL currently staged changes plus the version-file edits.
#   8.  Creates an annotated git tag.
#   9.  Prints the push command — does NOT push automatically.
#       Review the commit/tag first, then:
#         git push origin HEAD refs/tags/<tag>
#
# The pre-push hook (.githooks/pre-push) enforces tag↔CMakeLists consistency
# so even a manual 'git tag + git push' is caught before it reaches GitHub.
set -euo pipefail

# ── helpers ────────────────────────────────────────────────────────────────
die()  { echo "error: $*" >&2; exit 1; }
info() { echo "  $*"; }
step() { echo; echo "── $* ──"; }

# ── locate repo root ────────────────────────────────────────────────────────
REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null)" \
  || die "not inside a git repository"
cd "$REPO_ROOT"
[[ -f CMakeLists.txt ]]    || die "CMakeLists.txt not found at repo root"
[[ -f src/main/main.c ]]   || die "src/main/main.c not found"
[[ -f CHANGELOG.md ]]      || die "CHANGELOG.md not found"

# ── parse argument ──────────────────────────────────────────────────────────
[[ $# -ge 1 ]] || die "usage: $0 <version>  (e.g. 0.5.5-alpha)"
INPUT="$1"

# Strip leading 'v'
STRIPPED="${INPUT#v}"

# Semver part: everything before the first '-'
SEMVER="${STRIPPED%%-*}"

# Suffix: everything from the first '-' onward (may be empty)
if [[ "$STRIPPED" == *-* ]]; then
  SUFFIX="-${STRIPPED#*-}"
else
  SUFFIX=""
fi

# Validate semver X.Y.Z
[[ "$SEMVER" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] \
  || die "version must be MAJOR.MINOR.PATCH[-suffix] (got '$SEMVER')"

MAJOR="${SEMVER%%.*}"; REST="${SEMVER#*.}"
MINOR="${REST%%.*}";   PATCH="${REST#*.}"
TAG="v${SEMVER}${SUFFIX}"

echo
echo "Releasing: ${TAG}  (MAJOR=${MAJOR} MINOR=${MINOR} PATCH=${PATCH})"

# ── warn about unstaged changes ─────────────────────────────────────────────
UNSTAGED=$(git diff --name-only 2>/dev/null || true)
if [[ -n "$UNSTAGED" ]]; then
  echo
  echo "Warning: the following files have unstaged changes and will NOT be"
  echo "         included in the release commit:"
  echo "$UNSTAGED" | sed 's/^/    /'
  echo
  echo "  Stage them with 'git add' before running this script if needed."
  echo
  read -r -p "Continue anyway? [y/N] " yn
  [[ "$yn" =~ ^[Yy]$ ]] || { echo "Aborted."; exit 0; }
fi

# ── reject a tag that already exists ────────────────────────────────────────
if git rev-parse --verify "refs/tags/$TAG" >/dev/null 2>&1; then
  die "tag '$TAG' already exists locally — delete it first: git tag -d $TAG"
fi

step "Bumping CMakeLists.txt → VERSION ${SEMVER}"
# Replace the VERSION line inside the project() block (handles multi-line block)
perl -i -0777 -pe \
  "s/(project\s*\(\s*keel\b[^\)]*?VERSION\s+)\d+\.\d+\.\d+/\${1}${SEMVER}/s" \
  CMakeLists.txt
grep -q "VERSION ${SEMVER}" CMakeLists.txt \
  || die "CMakeLists.txt update failed — VERSION ${SEMVER} not found after edit"
info "CMakeLists.txt VERSION → ${SEMVER}"

step "Bumping src/main/main.c fallback guards"
sed -i "s/^#define KEEL_VERSION_MAJOR [0-9]*\b/#define KEEL_VERSION_MAJOR ${MAJOR}/" src/main/main.c
sed -i "s/^#define KEEL_VERSION_MINOR [0-9]*\b/#define KEEL_VERSION_MINOR ${MINOR}/" src/main/main.c
sed -i "s/^#define KEEL_VERSION_PATCH [0-9]*\b/#define KEEL_VERSION_PATCH ${PATCH}/" src/main/main.c
info "KEEL_VERSION_{MAJOR,MINOR,PATCH} → ${MAJOR}.${MINOR}.${PATCH}"

step "Updating CHANGELOG.md"
DATE=$(date +%Y-%m-%d)
SECTION_HEADER="## [${TAG}]"
if grep -qF "$SECTION_HEADER" CHANGELOG.md; then
  info "Section '${SECTION_HEADER}' already present — not adding stub."
else
  info "Adding stub section for ${TAG} (${DATE})"
  # Insert a stub immediately after the blank line that follows ## [Unreleased]
  perl -i -0777 -pe \
    "s|(## \[Unreleased\][^\n]*\n\n---\n)|\${1}\n## [${TAG}] \x{2014} ${DATE}\n\n<!-- TODO: replace this stub with the actual release notes before committing -->\n\n---\n|" \
    CHANGELOG.md
  echo
  echo "  A stub was added.  Open CHANGELOG.md now and fill it in, then"
  echo "  run 'git add CHANGELOG.md' before this script makes the commit."
  echo
  read -r -p "  Press Enter once CHANGELOG.md is ready, or Ctrl-C to abort: " _
fi

step "Activating .githooks"
if [[ -d .githooks ]]; then
  CURRENT_HOOKS=$(git config core.hooksPath 2>/dev/null || true)
  if [[ "$CURRENT_HOOKS" != ".githooks" ]]; then
    git config core.hooksPath .githooks
    info "git config core.hooksPath .githooks  ✓"
  else
    info "core.hooksPath already set to .githooks  ✓"
  fi
else
  info "No .githooks directory found — skipping."
fi

step "Staging version files"
git add CMakeLists.txt src/main/main.c CHANGELOG.md

if git diff --cached --quiet; then
  die "nothing staged to commit — all version files are already at ${SEMVER}?"
fi

step "Committing"
COMMIT_MSG="release: bump version to ${SEMVER} (${TAG})"
git commit -m "$COMMIT_MSG"
info "Committed: ${COMMIT_MSG}"

step "Creating annotated tag ${TAG}"
git tag -a "$TAG" -m "Release ${TAG}"
info "Tag created: ${TAG} → $(git rev-parse HEAD)"

echo
echo "══════════════════════════════════════════════════════════════════"
echo "  Done.  Review the commit and tag, then publish with:"
echo
echo "    git push origin HEAD refs/tags/${TAG}"
echo
echo "  The CI version-consistency gate will verify the tag matches"
echo "  CMakeLists.txt before building any packages."
echo "══════════════════════════════════════════════════════════════════"
