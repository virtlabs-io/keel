# Contributing to KEEL

Thank you for your interest in contributing to KEEL! This document describes the process for reporting bugs, proposing changes, and submitting pull requests.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Reporting Bugs](#reporting-bugs)
- [Security Vulnerabilities](#security-vulnerabilities)
- [Proposing Changes](#proposing-changes)
- [Development Setup](#development-setup)
- [Coding Standards](#coding-standards)
- [Testing Requirements](#testing-requirements)
- [Pull Request Process](#pull-request-process)
- [Commit Message Format](#commit-message-format)

---

## Code of Conduct

This project follows the [Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md). By participating, you agree to uphold these standards.

## Reporting Bugs

1. Check [existing issues](https://github.com/virtlabs-io/keel/issues) to avoid duplicates.
2. Use the **Bug Report** issue template and include:
   - KEEL version (`keel --version`)
   - OS and kernel version (`uname -a`)
   - Minimal reproduction steps
   - Expected vs. actual behaviour
   - Relevant logs (set `log_level = debug` in `keel.ini`)

## Security Vulnerabilities

**Do not open a public issue.** See [SECURITY.md](SECURITY.md) for the responsible disclosure process.

## Proposing Changes

For non-trivial changes, open an issue first to discuss the design. This avoids wasted effort on PRs that may not align with the project direction.

For minor fixes (typos, docs, small bugs), a PR is sufficient.

## Development Setup

```bash
# Clone
git clone https://github.com/virtlabs-io/keel.git
cd keel

# Install dependencies (Ubuntu 24.04)
sudo apt-get install -y build-essential cmake ninja-build pkg-config \
  liburing-dev libyaml-dev libssl-dev lua5.4 liblua5.4-dev python3 python3-dev \
  libzstd-dev libpam-dev libldap-dev lcov gcovr

# Generate test certificates
bash scripts/generate-test-certs.sh

# Debug build with tests
cmake -S . -B build-test -G Ninja \
  -DCMAKE_BUILD_TYPE=Debug \
  -DCMAKE_C_STANDARD=23 \
  -DKEEL_ENABLE_TESTS=ON \
  -DKEEL_ENABLE_COVERAGE=ON
cmake --build build-test

# Run unit tests
cd build-test && ctest --output-on-failure -j$(nproc)

# Run AddressSanitizer build
cmake -S . -B build-asan -G Ninja \
  -DCMAKE_BUILD_TYPE=Debug \
  -DCMAKE_C_STANDARD=23 \
  -DKEEL_ENABLE_TESTS=ON \
  -DKEEL_ENABLE_ASAN=ON
cmake --build build-asan && cd build-asan && ctest --output-on-failure -j$(nproc)
```

See [docs/TESTING.md](docs/TESTING.md) for the full test suite documentation, Docker integration tests, and the sanitizer matrix.

## Coding Standards

KEEL is written in **C23**. The build enforces `-Wall -Wextra -Werror -Wpedantic`.

- Follow the existing code style (2-space indentation, `snake_case` for functions and variables, `KEEL_` prefix for public API symbols)
- No external dependencies unless strictly necessary; prefer POSIX APIs and OpenSSL for crypto
- Every new subsystem must have unit tests; aim for all code paths covered
- Memory: use the arena, slab, or pool allocators; `malloc`/`free` only in init/teardown paths
- Use `keel_assert()` (not `assert()`) for invariants inside the reactor hot path

## Testing Requirements

All PRs must pass:

1. **`ctest`** — all unit and integration tests green
2. **AddressSanitizer + UBSan** (`build-asan`) — zero errors
3. **ThreadSanitizer** (`build-tsan`) — zero data races
4. **Python protocol + resilience suites** — see below
5. **Branch coverage ≥ 40%** and **line coverage ≥ 70%** — enforced by CI
6. New or changed features must include new or updated tests

### Running the Python Test Suites Locally

A Python test framework covers wire-protocol compliance, fault injection, and
regression correctness.  Install the dependency and run the suites:

```bash
# One-time setup
pip install psycopg2-binary

# Build with tests
cmake -S . -B build-host -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DKEEL_ENABLE_TESTS=ON
cmake --build build-host -j$(nproc)

# Minimum set required before opening a PR
python3 tests/run_tests.py \
  --suite protocol \
  --suite resilience \
  --build-dir build-host \
  --py-verbose

# Full suite list
python3 tests/run_tests.py --list
```

Reports are written to `tests/reports/report_latest.{html,json}`.

### Local Branch + Line Coverage

```bash
bash scripts/coverage.sh        # full build + test + report
SKIP_BUILD=1 bash scripts/coverage.sh   # skip build if already built
```

This generates `coverage-html/index.html` with per-line and per-branch
annotations, and prints a "dark corners" table ranking source files by how
little branch coverage they have.  Files near 0% branch coverage are the
highest-priority candidates for new tests.

For Docker integration tests, see [docs/TESTING.md](docs/TESTING.md#docker-integration-tests).
For full Python suite documentation, see [docs/TESTING.md](docs/TESTING.md#python-test-framework).

## Pull Request Process

1. Fork the repository and create a branch: `git checkout -b feat/my-feature`
2. Make your changes with tests
3. Run `ctest` and the ASan/TSan builds locally
4. Push and open a PR against `main`
5. A maintainer will review within a few business days
6. Address review comments; the PR will be squash-merged once approved

## Commit Message Format

Use [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<scope>): <short description>

[optional body]

[optional footer]
```

Types: `feat`, `fix`, `docs`, `test`, `chore`, `refactor`, `perf`, `ci`

Examples:
```
feat(router): add range-based shard strategy
fix(tls): prevent cert hot-reload race on SIGHUP
docs(TESTING.md): update docker integration test table
test(sharding): add scatter aggregation edge cases
```
