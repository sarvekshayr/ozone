# Temporary Changes for Fork Testing

This file documents the temporary changes made to enable testing in `sarvekshayr/ozone` fork.

## Branch: HDDS-11072-test-fork

## Changes Made

### 1. `.github/workflows/ci.yml` - Line ~286

**Changed**: Allow `update-ozone-site-config-doc` job to run in fork

**Before**:
```yaml
if: |
  github.repository == 'apache/ozone' && 
  github.event_name == 'push' && 
  github.ref_name == 'master' &&
  !startsWith(github.event.head_commit.message, '[Auto]')
```

**After**:
```yaml
if: |
  github.event_name == 'push' &&
  !startsWith(github.event.head_commit.message, '[Auto]') &&
  (github.repository == 'apache/ozone' && github.ref_name == 'master' ||
   github.repository == 'sarvekshayr/ozone' && github.ref_name == 'HDDS-11072-test-fork')
```

### 2. No changes to ozone-site base branch

**Note**: Both apache/ozone and forks use `HDDS-9225-website-v2` as the base branch for ozone-site PRs.

## What This Enables

1. ✅ Workflow runs on `HDDS-11072-test-fork` branch in `sarvekshayr/ozone`
2. ✅ Clones `sarvekshayr/ozone-site` (HDDS-9225-website-v2 branch)
3. ✅ Creates PRs in both `sarvekshayr/ozone` and `sarvekshayr/ozone-site`
4. ✅ Uses `HDDS-9225-website-v2` as base branch for ozone-site (same as production)

## Testing Steps

1. Push changes to `HDDS-11072-test-fork` branch
2. Wait for GitHub Actions to complete (~10-15 minutes)
3. Verify PRs are created in:
   - `sarvekshayr/ozone` (automated-config-doc-update → HDDS-11072-test-fork)
   - `sarvekshayr/ozone-site` (automated-config-doc-update → HDDS-9225-website-v2)

## Cleanup After Testing

These changes should be reverted before merging to `apache/ozone`:

```bash
# Revert to original conditions
git checkout HDDS-11072
# Or manually revert the three changes above
```

## Files Modified

- `.github/workflows/ci.yml` (1 change: enable ozone-site job for fork)
- This documentation file (for reference)

---

**Note**: This is a temporary configuration for testing purposes only.
