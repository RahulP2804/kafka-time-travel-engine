# Contributing

Thanks for your interest in contributing to kafka-time-travel-engine.

## Getting Started

1. Fork the repo and create a feature branch from `main`
2. Follow the existing package structure: `api → engine → filter/transformer → kafka → storage`
3. Add unit tests for new filter operators or transformer logic
4. Add integration tests using Testcontainers for any new Kafka or Redis interactions
5. Keep DTOs as Java records; avoid Lombok
6. No hardcoded credentials — all config via environment variables

## Code Style

- Java 21 idioms: records, sealed interfaces, pattern matching where appropriate
- Explicit constructors over frameworks (no Lombok)
- Meaningful method names; avoid abbreviations in engine and filter logic
- Comments in complex Kafka offset logic should explain *why*, not *what*

## Pull Request Checklist

- [ ] Tests pass: `mvn verify`
- [ ] No new hardcoded config values
- [ ] README updated if new endpoints or env vars are added
- [ ] `.env.example` updated if new environment variables are introduced

## Reporting Issues

Open a GitHub issue with: expected behaviour, actual behaviour, and the minimal replay request that triggers it.
