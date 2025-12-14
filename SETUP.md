# Axis Setup Guide

Complete guide to setting up and running Axis replication system.

## Prerequisites

- Python 3.9+
- PostgreSQL 12+
- RabbitMQ 3.8+
- Docker (optional, for easy setup)

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Setup PostgreSQL (Docker)

```bash
# Start source database
docker run -d \
  --name postgres-source \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=source_db \
  -p 5432:5432 \
  postgres:14

# Start replica database
docker run -d \
  --name postgres-replica \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=replica_db \
  -p 5433:5432 \
  postgres:14
```

### 3. Setup RabbitMQ (Docker)

```bash
docker run -d \
  --name rabbitmq \
  -p 5672:5672 \
  -p 15672:15672 \
  rabbitmq:3-management
```

Access RabbitMQ management UI at http://localhost:15672 (guest/guest)

### 4. Configure Environment

```bash
# Copy example configuration
cp .env.example .env

# Edit .env with your settings
nano .env
```

### 5. Setup Database Triggers

Connect to your source database and run:

```sql
-- Create a test table
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    name VARCHAR(255),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- The trigger setup will be done automatically by Axis
-- when you run it for the first time
```

### 6. Run Axis

#### Option A: Full Orchestrator Mode (Recommended)

```bash
# Run with full orchestration and replication
python src/cli.py --replicas "postgresql://postgres:postgres@localhost:5433/replica_db"
```

#### Option B: Simple Publisher Mode

```bash
# Run in publish-only mode
python src/cli.py --simple
```

### 7. Test Replication

In another terminal, connect to source database:

```bash
docker exec -it postgres-source psql -U postgres -d source_db
```

Insert some test data:

```sql
INSERT INTO users (email, name) VALUES 
    ('alice@example.com', 'Alice'),
    ('bob@example.com', 'Bob');

UPDATE users SET name = 'Alice Smith' WHERE email = 'alice@example.com';

DELETE FROM users WHERE email = 'bob@example.com';
```

Check the replica database:

```bash
docker exec -it postgres-replica psql -U postgres -d replica_db
```

```sql
SELECT * FROM users;
```

## Advanced Configuration

### Custom Tables to Replicate

Edit `.env`:

```env
TABLES_TO_REPLICATE=users,orders,products
```

### Batch Settings

```env
BATCH_SIZE=50
BATCH_TIMEOUT=10.0
```

### Queue Settings

```env
MAX_QUEUE_SIZE=50000
PREFETCH_COUNT=20
```

## Monitoring

### Check Logs

Logs are written to console with timestamps. Look for:

- ✓ Connection status
- Event counts
- Errors and warnings
- Backpressure alerts

### RabbitMQ Management

Access http://localhost:15672 to monitor:

- Message rates
- Queue depths
- Connections
- Exchanges

### Health Check

The orchestrator provides health metrics:

```python
# In your code
health = await orchestrator.health_check()
print(health)
```

### Metrics

```python
# Get detailed metrics
metrics = await orchestrator.get_metrics()
print(metrics)
```

## Troubleshooting

### Connection Issues

**PostgreSQL connection failed:**
- Check DSN format: `postgresql://user:password@host:port/database`
- Verify PostgreSQL is running: `docker ps`
- Check firewall/network settings

**RabbitMQ connection failed:**
- Check RabbitMQ is running: `docker ps`
- Verify URL format: `amqp://user:password@host:port/`
- Check credentials (default: guest/guest)

### Replication Not Working

**No events being published:**
- Verify triggers are created: `\df` in psql
- Check table names in TABLES_TO_REPLICATE
- Look for errors in logs

**Events published but not replicated:**
- Check replica database connection
- Verify replica queue is consuming
- Check RabbitMQ management UI for messages

### Performance Issues

**High latency:**
- Increase BATCH_SIZE
- Add more replica consumers
- Check network latency

**Queue buildup:**
- Backpressure is working! System is under load
- Scale horizontally with more consumers
- Optimize batch settings

**High memory usage:**
- Reduce MAX_QUEUE_SIZE
- Reduce BATCH_SIZE
- Enable state cleanup

## Production Deployment

### 1. Use Connection Pooling

Already configured! Default pools:
- Min: 2 connections
- Max: 10 connections

### 2. Enable State Persistence

Set in `.env`:

```env
STATE_FILE=/var/lib/axis/state.json
```

### 3. Setup Monitoring

- Use Prometheus/Grafana (coming soon)
- Monitor RabbitMQ queues
- Track PostgreSQL replication lag
- Set up alerts for failures

### 4. High Availability

- Run multiple Axis instances
- Use RabbitMQ clustering
- Setup PostgreSQL streaming replication as backup
- Implement leader election (coming soon)

### 5. Security

- Use SSL/TLS for all connections
- Rotate credentials regularly
- Enable authentication on RabbitMQ
- Use SSL for PostgreSQL connections
- Implement PII masking (see examples)

## Next Steps

- Read the [Architecture Guide](docs/architecture.md) (coming soon)
- Check [Examples](examples/) for advanced usage
- Configure [Transformations](examples/example_with_transformer.py)
- Setup [Message Routing](examples/example_with_dispatcher.py)
- Write tests for your setup

## Getting Help

- Check the logs for detailed error messages
- Review examples in the `examples/` directory
- Check RabbitMQ management UI for queue status
- Enable debug logging in `.env`
