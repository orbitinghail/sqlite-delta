# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is `sqlite-delta`, a collection of Change Data Capture (CDC) patterns for SQLite databases. The project provides reusable SQL patterns that developers can copy and adapt into their SQLite-based applications.

## Essential Commands

### Development Commands

```bash
# Install dependencies
uv sync --dev

# Lint SQL files (must pass for PRs)
uv run poe lint

# Format SQL files
uv run poe fmt

# Test all patterns (must pass for PRs)
uv run poe test
```

### Dependencies

- **uv** - Python package manager (required)
- **sqlite3** - SQLite command line tool (required)

## Repository layout

### Patterns (`/patterns/`)

**trigger-lite/** - General-purpose CDC using SQLite triggers
**three-phase/** - Delta compression using triple-buffering

### Pattern file structure

- `pattern.sql` - Complete SQL implementation (legacy)
- `pattern.py` - Python implementation with templated SQL and comprehensive tests
- `README.md` - Additional pattern documentation and usage examples

## Development Guidelines

### Code Quality

- All SQL must pass `uv run poe lint` (SQLFluff with SQLite dialect)
- All patterns must pass `uv run poe test` (runs pattern.py files)
- PRs require passing linting, formatting, and testing checks

### SQL Patterns

- Patterns use pure SQL with no runtime dependencies
- Designed for SQLite 3.x compatibility
- Extensively documented with inline comments
- Include complete examples and usage patterns
