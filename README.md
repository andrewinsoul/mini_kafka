# Mini-Kafka

A small Kafka-inspired event log built from scratch with **Elixir and OTP**.

The goal isn't to recreate Kafka, but to understand the core ideas behind event storage and consumption by building a simplified version ourselves.

## What it covers

* Append-only event log
* Event offsets
* Reading events from an offset
* Event consumer
* Consumer offsets
* Persistent state recovery
* OTP supervision and process recovery

## Running

```bash
git clone <repo-url>
cd mini_kafka
mix deps.get
mix run --no-halt
```

You can interact with the system through IEx:

```bash
iex -S mix
```

## Project Structure

```text
lib/
└── mini_kafka/
    ├── log.ex
    └── consumer.ex
```

`MiniKafka.Log` manages the append-only event log, while `MiniKafka.Consumer` reads and processes events from the log.

This project is intentionally simple and far from Kafka's full architecture. Concepts such as partitions, consumer groups, replication, leader selection, retention, and rebalancing are outside its scope.

Built to explore **Kafka concepts through Elixir and OTP**.
