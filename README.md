# Trigger Logging Analysis

```bash
export TRIGGER_LOGGING=/Users/carlpulley/Downloads/nft2-febr-24-test-run-logs/diexec-trigger-cleartream-dev-nft2.log

log-parse.bash "$TRIGGER_LOGGING" >"${TRIGGER_LOGGING}.json"

python3 log-indexing.py --file="${TRIGGER_LOGGING}.json" >"${TRIGGER_LOGGING}.indexed.json"

python3 log-analysis.py --file="${TRIGGER_LOGGING}.indexed.json" >"${TRIGGER_LOGGING}.analysis.md"
```

TODO:
- [ ] add in a nix configuration script for setting up analysis environment
