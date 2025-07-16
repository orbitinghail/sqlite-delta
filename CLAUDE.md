# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is `sqlite-delta`, a collection of Change Data Capture (CDC) patterns for SQLite databases. **This repository is educational** - the patterns are designed to be studied, understood, and adapted rather than used directly in production.

The Python implementations serve as working examples with comprehensive tests to demonstrate the concepts. Users should read and understand the patterns, then re-implement them in their own codebase tailored to their specific tables and requirements.

## Essential Commands

### Development Commands

```bash
# Install dependencies
uv sync

# Lint python files
uv run poe lint

# Format python files
uv run poe fmt

# Test all patterns
uv run poe test

# Run a specific python file
uv run path/to/python/file.py
```

Run lint, fmt, and test before finishing any task to verify correctness.

### Dependencies

- **uv** - Python package manager and task runner (required)

## Repository layout

### Patterns (`/patterns/`)

**trigger-lite/** - General-purpose CDC using SQLite triggers
**three-phase/** - Delta compression using triple-buffering

### Pattern file structure

- `pattern.py` - Complete Python implementation with comprehensive tests and examples
- `README.md` - Pattern documentation and usage examples
