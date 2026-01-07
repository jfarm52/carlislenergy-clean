#!/bin/bash

cd /Users/jfarmfiit/Documents/GitHub/carlislenergy-clean

LOG_FILE="git_result.log"

{
    echo "=== Current branch ==="
    git branch --show-current

    echo "=== Git status ==="
    git status --short

    echo "=== Staging all changes ==="
    git add -A

    echo "=== Committing ==="
    git commit -m "Complete bill extraction: universal regex, TOU, status system, expand/collapse, no date limits" 2>&1 || echo "Commit note: nothing to commit or already committed"

    echo "=== Switching to test-main ==="
    git checkout test-main 2>&1

    echo "=== Merging sce-extraction into test-main ==="
    git merge sce-extraction -m "Merge sce-extraction: universal regex, TOU improvements, date filter removal" 2>&1

    echo "=== Pushing to GitHub ==="
    git push origin test-main 2>&1

    echo "=== Done! ==="
    echo "Current branch:"
    git branch --show-current
} > "$LOG_FILE" 2>&1

echo "Script completed - check git_result.log"
