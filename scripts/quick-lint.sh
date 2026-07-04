#!/bin/bash

set -e

# Check if goimports is installed
if ! command -v goimports &> /dev/null; then
    echo "Installing goimports..."
    go install golang.org/x/tools/cmd/goimports@latest
    export PATH="$(go env GOPATH)/bin:$PATH"
fi

# Check if golint is installed
if ! command -v golint &> /dev/null; then
    echo "Installing golint..."
    go install golang.org/x/lint/golint@latest
    export PATH="$(go env GOPATH)/bin:$PATH"
fi

# Run quick lint checks
CGO_ENABLED=1 goimports -d -local "github.com/premchandkpc/FlowRulZ" ./server/internal

if [ $? -eq 0 ]; then
    echo "✓ goimports check passed"
else
    echo "✗ goimports check failed - see changes above"
    exit 1
fi

CGO_ENABLED=1 golint -set_exit_status ./server/internal/pkg

if [ $? -eq 0 ]; then
    echo "✓ golint check passed"
else
    echo "✗ golint check failed - see output above"
    exit 1
fi

echo "✓ All quick lint checks passed"
