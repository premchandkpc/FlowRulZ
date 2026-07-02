# Cluster Debugging

Use this skill when investigating FlowRulZ cluster behavior, leader election, plan distribution, membership, or replica coordination issues.

## When to use
- Leader election or term transitions appear incorrect
- Plan distribution or acknowledgements are delayed or inconsistent
- Membership changes or node heartbeats behave unexpectedly
- The cluster does not converge after joins, leaves, or failures

## Focus areas
- Leader election flow and term handling
- Membership tracking and heartbeat behavior
- Plan/ack delivery and quorum logic
- Rebalance and partition ownership transitions

## Output expectations
- Briefly isolate the likely subsystem involved
- Trace the control flow from the failing path to the relevant node logic
- Recommend the smallest fix with validation steps
