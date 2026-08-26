# opentmf-outbox-service

Transactional outbox as a Spring Boot starter: library-owned table DDL
(Liquibase), an at-least-once relay with pluggable publishers (Kafka default),
and an `/ops` maintenance surface (prune / unpark / list).

Under construction — the initial extraction is in progress. This README will
become the library developer's guide: guarantees and mechanism, a five-minute
adoption walkthrough, the publisher SPI contract, and the migration guide from
a hand-written outbox.
