#!/bin/bash
set -e

# Test script for sqlite-delta patterns
# Discovers and runs all pattern.py files in the patterns/ directory

echo "🧪 Running sqlite-delta pattern tests..."
echo

# Find all pattern.py files in the patterns directory
pattern_files=$(find patterns -name "pattern.py" -type f | sort)

if [ -z "$pattern_files" ]; then
    echo "❌ No pattern.py files found in patterns/ directory"
    exit 1
fi

# Track test results
total_patterns=0
passed_patterns=0

# Run each pattern test
for pattern_file in $pattern_files; do
    pattern_name=$(dirname "$pattern_file" | sed 's|patterns/||')
    echo "📋 Testing pattern: $pattern_name"
    echo "   File: $pattern_file"
    
    total_patterns=$((total_patterns + 1))
    
    # Run the pattern with uv
    if uv run python "$pattern_file"; then
        echo "✅ $pattern_name: PASSED"
        passed_patterns=$((passed_patterns + 1))
    else
        echo "❌ $pattern_name: FAILED"
    fi
    
    echo
done

# Summary
echo "════════════════════════════════════════"
echo "📊 Test Summary:"
echo "   Total patterns: $total_patterns"
echo "   Passed: $passed_patterns" 
echo "   Failed: $((total_patterns - passed_patterns))"

if [ $passed_patterns -eq $total_patterns ]; then
    echo "🎉 All tests passed!"
    exit 0
else
    echo "💥 Some tests failed!"
    exit 1
fi