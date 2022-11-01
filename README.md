![Erlang CI](https://github.com/electric-sql/vaxine/workflows/CI/badge.svg)
[![Coverage Status](https://coveralls.io/repos/github/electric-sql/vaxine/badge.svg?branch=main)](https://coveralls.io/github/electric-sql/vaxine?branch=main)
[![License - Apache 2.0](https://img.shields.io/badge/license-Apache-green)](./blob/main/LICENSE.md)
![Status - Alpha](https://img.shields.io/badge/status-alpha-red)

# Vaxine

Welcome to the Vaxine source code repository. Vaxine is a rich-CRDT database system based on AntidoteDB. It's used as the core replication layer for [ElectricSQL](https://electric-sql.com).

## About Antidote

AntidoteDB is a planet scale, highly available, transactional database. Antidote implements the [Cure protocol](https://ieeexplore.ieee.org/document/7536539/) of transactional causal+ consistency based on [CRDTs](https://crdt.tech).

More information:

- [Antidote website](https://www.antidotedb.eu)
- [Documentation](https://antidotedb.gitbook.io/documentation)

Antidote is the reference platform of the [SyncFree](https://syncfree.lip6.fr/) and the [LightKone](https://www.lightkone.eu/) european projects.

## Community

If you have any questions, please raise them on the [ElectricSQL community](https://electric-sql.com/about/community) channels.

## Dependencies

- Erlang >= 24.0
- inotifywait/kqueue/fsevents depending on your OS
