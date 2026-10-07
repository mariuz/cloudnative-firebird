# Upstream reports

Firebird problems found while building this operator, written up as issues for the
[Firebird tracker](https://github.com/FirebirdSQL/firebird/issues). Each one has a
reproduction script in [`hack/repro/`](../../hack/repro) that needs only Docker. Background and
how the operator handles them: [ISSUES.md](../../ISSUES.md).

| Report | ISSUES.md | Status |
|--------|-----------|--------|
| [Deadlock creating the replication manager](01-replication-manager-header-deadlock.md) (stacks: [full output](01-replication-manager-header-deadlock-stacks.txt)) | 1 | ready to file |
| [Commit journaled before its TIP state](02-commit-journaled-before-tip.md) | 2 | ready to file |

Once filed, replace "ready to file" with the tracker link, and add it to ISSUES.md.
