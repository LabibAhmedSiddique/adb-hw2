# ADB Homework 2 — Deadlocks, restarts and starvation

Based on the supplied professor Blocking.java. Advanced Databases, WS 2026/27, University of Salzburg.

## Files
- Blocking.java — original professor code (Part 2), unchanged
- RestartBlocking.java — retries transactions aborted with PostgreSQL SQLSTATE 40P01 / 40001 until they commit (Part 3)
- NoLiveLocks.java — like RestartBlocking, plus a fair ReentrantReadWriteLock gate: after 3 aborts a task runs alone, so every task commits in at most 4 attempts (Part 4)
- ConnectionFactory.java — local PostgreSQL connection adapter

## Configuration
Requires Java 21, PostgreSQL 16 and the PostgreSQL JDBC driver jar in `lib/`.
Set your database URL, user and password in ConnectionFactory.java.
Run Blocking without arguments, then RestartBlocking and NoLiveLocks with optional arguments `[tasks] [queries] [threads]` (default `30 3 4`). All programs generate 50,000 rows. Run one at a time: each recreates blocking_data.

## Notes
Tested against a local PostgreSQL 16 database; deadlock counts and runtimes vary from run to run. The fair gate in NoLiveLocks only coordinates tasks within this Java process, not external clients. Preserve the original project's licensing and acknowledge outside assistance according to class policy.