# peekd

Standalone Ethereum P2P network diagnostic tool that collects and analyzes beacon chain gossip messages — without requiring a CL node.

## Architecture

```
 Ethereum P2P Network
        │
   ┌────┴───┐
   │ DiscV5 │  UDP peer discovery
   └────┬───┘
        │ discovered peers
   ┌────┴───┐
   │ Dialer │  TCP connection
   └────┬───┘
        │ connected peers
   ┌────┴──────┐
   │ GossipSub │  Subscribe to beacon chain topics
   └────┬──────┘
        │ messages
   ┌────┴──────┐
   │ Processor │  Slot-based caching, latency calculation, deduplication tracking
   └────┬──────┘
        │ batch flush per slot
   ┌────┴───────┐
   │ ClickHouse │  Time-series storage + Materialized View (hourly rollup with p50/p90/p95)
   └────────────┘
```

## Key Design Decisions

- **No CL node dependency**: Connects directly to the Ethereum P2P network using libp2p, replacing the CL node with high-reliability peers via DiscV5.
- **Slot-based batch processing**: Messages are cached per slot and flushed to ClickHouse once the slot is complete, minimizing write operations.
- **ClickHouse Materialized View**: Hourly aggregation (latency percentiles, duplication metrics) is computed at the DB level, keeping the application lightweight.
- **Multi-region support**: `node_region` and `node_alias` fields allow deploying multiple instances across regions for comparative analysis.

## Supported Message Types

| Category | Topics |
|----------|--------|
| Global | beacon_block, aggregate_and_proof, sync_contribution_and_proof, proposer_slashing, attester_slashing, voluntary_exit, bls_to_execution_change |
| Subnet | beacon_attestation (64 subnets), sync_committee (4 subnets), blob_sidecar (6 subnets) |

## Quick Start

```bash
cp .env.example .env
docker compose up -d
```

## Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `PEEKD_NETWORK` | Ethereum network (`mainnet`, `hoodi`) | `mainnet` |
| `PEEKD_LISTEN_IP` | Listen IP address | `127.0.0.1` |
| `PEEKD_UDP_PORT` / `PEEKD_TCP_PORT` | DiscV5 / libp2p port | `9090` |
| `PEEKD_ESTIMATE_ACTIVE_VALIDATORS` | Estimated active validator count (for peer scoring) | `1000000` |
| `PEEKD_NODE_REGION` | Node region label | `local` |
| `PEEKD_NODE_ALIAS` | Node alias label | `local` |
| `PEEKD_DB_*` | ClickHouse connection settings | see `.env.example` |
