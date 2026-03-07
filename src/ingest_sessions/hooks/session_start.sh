#!/usr/bin/env bash
# LCM context recovery hook for Claude Code SessionStart.
#
# Called by Claude Code after /clear (and on session start).
# Queries the ingest_sessions MCP server for the summary DAG context
# and returns it as additionalContext for injection.
#
# Prerequisites:
#   - ingest-sessions-server running on localhost:8741
#   - curl available
#
# See docs/plans/2026-03-07-lcm-summary-dag.md

set -euo pipefail

# The session ID is passed via environment by Claude Code
SESSION_ID="${CLAUDE_SESSION_ID:-}"

if [ -z "$SESSION_ID" ]; then
    exit 0  # No session ID, nothing to do
fi

# Query the context tool via the MCP HTTP endpoint
RESPONSE=$(curl -s -X POST "http://127.0.0.1:8741/mcp" \
    -H "Content-Type: application/json" \
    -d "{
        \"jsonrpc\": \"2.0\",
        \"method\": \"tools/call\",
        \"params\": {
            \"name\": \"context\",
            \"arguments\": {\"session_id\": \"$SESSION_ID\"}
        },
        \"id\": 1
    }" 2>/dev/null || echo "")

if [ -z "$RESPONSE" ]; then
    exit 0  # Server not reachable, fail silently
fi

# Extract the context string from the response
CONTEXT=$(echo "$RESPONSE" | python3 -c "
import sys, json
try:
    resp = json.load(sys.stdin)
    content = resp.get('result', {}).get('content', [{}])
    if isinstance(content, list) and len(content) > 0:
        text = content[0].get('text', '{}')
        ctx = json.loads(text).get('context')
        if ctx:
            # Output the hook response with additionalContext
            print(json.dumps({
                'hookSpecificOutput': {
                    'hookEventName': 'SessionStart',
                    'additionalContext': ctx
                }
            }))
except Exception:
    pass
" 2>/dev/null || echo "")

if [ -n "$CONTEXT" ]; then
    echo "$CONTEXT"
fi
