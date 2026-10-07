#!/usr/bin/env bash
# red_gate.sh — CI gate for the "red" acceptance suite (red_test.go, build tag `red`).
#
# The suite is a set of repros of known bugs: every listed test is EXPECTED to
# fail today, and each fix must shrink the allow-fail list in the same PR.
#
# Exit 0 ONLY IF the set of failing tests equals the allow-fail list exactly:
#   * every allow-listed test fails, and
#   * no other test fails or skips.
# Otherwise exit 1 with a precise message:
#   * 'unexpected failure: <name>'                        — unlisted fail/skip
#   * 'stale allow-list entry: <name> now passes — remove it in this PR'
#
# Override the list (for experiments / negative tests) with RED_GATE_ALLOW,
# a space- or newline-separated list of test names.
# Set RED_GATE_VERBOSE=1 to dump the full `go test` output.

set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.." || exit 1

DEFAULT_ALLOW='
TestRed_RT01_DrainingSlotNotSelected
TestRed_RT02_DeadWANDoesNotAttractTraffic
TestRed_RT03_ConnCountNeverNegative
TestRed_RT06_ConnectPipelinedBytesPreserved
TestRed_RT07_HalfCloseKeepsReverseDirectionOpen
TestRed_RT08_FailoverOnDialFailure
TestRed_RT09_CandidatePoolDedupes
TestRed_RT10_DropAndReplaceKeepsOldWANUntilCandidateValidated
TestRed_RT11_FullPoolStillEvaluatesCandidates
'

ALLOW_RAW="${RED_GATE_ALLOW-$DEFAULT_ALLOW}"

# Split the allow list into an array (bash 3.2 compatible: no assoc arrays/readarray).
allow=()
for tok in $ALLOW_RAW; do
  allow+=("$tok")
done

if [ "${#allow[@]}" -eq 0 ]; then
  echo "red gate: allow-fail list is empty" >&2
  exit 1
fi

logfile="$(mktemp "${TMPDIR:-/tmp}/red_gate.XXXXXX")" || exit 1
trap 'rm -f "$logfile"' EXIT

echo "== red gate: go test -tags red -race -count=1 -run 'TestRed_' -v ."
go test -tags red -race -count=1 -run 'TestRed_' -v . >"$logfile" 2>&1
test_rc=$?

if [ "${RED_GATE_VERBOSE:-0}" = "1" ]; then
  cat "$logfile"
fi

# Parse per-test verdicts: lines like '--- FAIL: TestRed_RT01_... (0.02s)'.
failed=()
passed=()
skipped=()
while IFS=' ' read -r verdict name; do
  case "$verdict" in
    FAIL) failed+=("$name") ;;
    PASS) passed+=("$name") ;;
    SKIP) skipped+=("$name") ;;
  esac
done < <(grep -E '^--- (FAIL|PASS|SKIP): TestRed_[A-Za-z0-9_]+' "$logfile" \
           | sed -E 's/^--- ([A-Z]+): ([A-Za-z0-9_]+).*/\1 \2/' \
           | awk '!seen[$1 FS $2]++')

total_results=$(( ${#failed[@]} + ${#passed[@]} + ${#skipped[@]} ))

if [ "$total_results" -eq 0 ]; then
  echo "red gate: FAIL — no TestRed_* results produced (build/vet failure? go test exit=$test_rc)"
  echo "---- go test output ----"
  cat "$logfile"
  exit 1
fi

contains() {
  # contains <needle> <item...>
  local needle="$1" item
  shift
  for item in "$@"; do
    [ "$item" = "$needle" ] && return 0
  done
  return 1
}

problems=()

# Direction 1: every allow-listed test must fail (not pass, not skip, not vanish).
for name in "${allow[@]}"; do
  if contains "$name" ${failed[@]+"${failed[@]}"}; then
    continue
  elif contains "$name" ${passed[@]+"${passed[@]}"}; then
    problems+=("stale allow-list entry: $name now passes — remove it in this PR")
  elif contains "$name" ${skipped[@]+"${skipped[@]}"}; then
    problems+=("stale allow-list entry: $name now skips (never skip a red test) — remove it in this PR")
  else
    problems+=("stale allow-list entry: $name did not run — remove it in this PR")
  fi
done

# Direction 2: no unlisted test may fail or skip.
for name in ${failed[@]+"${failed[@]}"} ${skipped[@]+"${skipped[@]}"}; do
  if ! contains "$name" "${allow[@]}"; then
    problems+=("unexpected failure: $name")
  fi
done

echo ""
echo "== red gate results =="
for name in "${allow[@]}"; do
  if contains "$name" ${failed[@]+"${failed[@]}"}; then
    echo "  EXPECTED FAIL  $name"
  elif contains "$name" ${passed[@]+"${passed[@]}"}; then
    echo "  PASS           $name"
  elif contains "$name" ${skipped[@]+"${skipped[@]}"}; then
    echo "  SKIP           $name"
  else
    echo "  MISSING        $name"
  fi
done
echo "  failing=${#failed[@]} passing=${#passed[@]} skipping=${#skipped[@]} allow=${#allow[@]} (go test exit=$test_rc)"

if [ "${#problems[@]}" -gt 0 ]; then
  echo "red gate: FAIL"
  for p in "${problems[@]}"; do
    echo "  $p"
  done
  echo "---- full go test output ----"
  cat "$logfile"
  exit 1
fi

echo "red gate: OK — failing set exactly matches the ${#allow[@]} allow-listed tests"
exit 0
