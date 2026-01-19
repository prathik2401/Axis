# Axis Testing Guide - Remote Database Setup

## Test Environment Setup

### Your Configuration
- **Database Host:** 192.168.29.5
- **Your Machine (Axis):** Local development machine
- **Network:** Both machines should be on the same network

---

## Step 1: PostgreSQL Setup on 192.168.29.5

### Required PostgreSQL Instances

You need to set up **2 PostgreSQL instances** on 192.168.29.5:

1. **Source Database** (Port 5432)
2. **Replica Database** (Port 5433)

### Installation Options

#### Option A: Docker (Recommended)

```bash
# SSH into 192.168.29.5
ssh user@192.168.29.5

# Start Source Database (Port 5432)
docker run -d \
  --name postgres-source \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_DB=source_db \
  -p 5432:5432 \
  -v postgres_source_data:/var/lib/postgresql/data \
  postgres:14

# Start Replica Database (Port 5433)
docker run -d \
  --name postgres-replica \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_DB=replica_db \
  -p 5433:5432 \
  -v postgres_replica_data:/var/lib/postgresql/data \
  postgres:14

# Verify both are running
docker ps
```

#### Option B: Native PostgreSQL Installation

If you prefer native installation, ensure:
- PostgreSQL 12+ is installed
- Two separate instances on ports 5432 and 5433
- Configure `postgresql.conf` to allow remote connections
- Update `pg_hba.conf` to allow connections from your IP

---

## Step 2: RabbitMQ Setup

You can run RabbitMQ either on 192.168.29.5 OR on your local machine.

### Option A: RabbitMQ on 192.168.29.5 (Recommended)

```bash
# On 192.168.29.5
docker run -d \
  --name rabbitmq \
  -e RABBITMQ_DEFAULT_USER=guest \
  -e RABBITMQ_DEFAULT_PASS=guest \
  -p 5672:5672 \
  -p 15672:15672 \
  rabbitmq:3-management

# Verify it's running
docker ps | grep rabbitmq
```

Access RabbitMQ Management UI: http://192.168.29.5:15672 (guest/guest)

### Option B: RabbitMQ on Local Machine

```bash
# On your local machine
docker run -d \
  --name rabbitmq \
  -p 5672:5672 \
  -p 15672:15672 \
  rabbitmq:3-management
```

Access: http://localhost:15672

---

## Step 3: Configure PostgreSQL for Remote Access

### On 192.168.29.5, configure PostgreSQL to accept remote connections:

```bash
# For Docker PostgreSQL, it's already configured!
# Docker containers automatically listen on 0.0.0.0

# Verify connectivity from your machine
psql -h 192.168.29.5 -U postgres -d source_db -p 5432
# Enter password: postgres
```

---

## Step 4: Create Test Database Schema

Connect to source database and create test tables:

```bash
# From your local machine
psql -h 192.168.29.5 -U postgres -d source_db -p 5432
```

Or using Docker exec on the remote machine:

```bash
# On 192.168.29.5
docker exec -it postgres-source psql -U postgres -d source_db
```

Then run this SQL:

```sql
-- Create test tables
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) NOT NULL UNIQUE,
    name VARCHAR(255) NOT NULL,
    phone VARCHAR(20),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    product_name VARCHAR(255) NOT NULL,
    quantity INTEGER NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    status VARCHAR(50) DEFAULT 'pending',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    price DECIMAL(10, 2) NOT NULL,
    stock INTEGER DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Insert some test data
INSERT INTO users (email, name, phone) VALUES
    ('john@example.com', 'John Doe', '555-1234'),
    ('jane@example.com', 'Jane Smith', '555-5678'),
    ('bob@example.com', 'Bob Johnson', '555-9012');

INSERT INTO products (name, description, price, stock) VALUES
    ('Laptop', 'High-performance laptop', 1299.99, 50),
    ('Mouse', 'Wireless mouse', 29.99, 200),
    ('Keyboard', 'Mechanical keyboard', 149.99, 100);

-- Verify data
SELECT * FROM users;
SELECT * FROM products;
```

---

## Step 5: Configure Axis on Your Local Machine

### Create .env file:

```bash
# In your Axis project directory
cd C:\Users\saipr\Projects\Axis
```

Create `.env` file with the following content:

```env
# PostgreSQL Source Database
PG_DSN=postgresql://postgres:postgres@192.168.29.5:5432/source_db
PG_CHANNEL=axis_changes

# RabbitMQ Configuration (adjust based on where you installed it)
# If RabbitMQ is on 192.168.29.5:
RMQ_URL=amqp://guest:guest@192.168.29.5:5672/

# If RabbitMQ is local:
# RMQ_URL=amqp://guest:guest@localhost:5672/

RMQ_EXCHANGE=axis_replication
RMQ_EXCHANGE_TYPE=fanout

# Replication Settings
TABLES_TO_REPLICATE=users,orders,products
```

---

## Step 6: Install Axis Dependencies

```powershell
# Ensure you're in the Axis directory
cd C:\Users\saipr\Projects\Axis

# Install all dependencies
pip install -r requirements.txt

# Verify installation
python -c "import asyncpg, aio_pika, prometheus_client, aiohttp; print('All dependencies installed!')"
```

---

## Step 7: Run Axis - Test 1 (Basic Connectivity)

### Test database connectivity first:

```powershell
# Test PostgreSQL connectivity
python -c "import asyncio; import asyncpg; asyncio.run(asyncpg.connect('postgresql://postgres:postgres@192.168.29.5:5432/source_db'))"
```

If this works, you should see no errors.

---

## Step 8: Run Axis - Test 2 (Simple Publisher Mode)

Start Axis in simple mode first:

```powershell
python src/cli.py --simple
```

Expected output:
```
============================================================
AXIS - Async PostgreSQL Replication System
============================================================
Mode: Simple Publisher
Source DB: 192.168.29.5:5432/source_db
RabbitMQ: 192.168.29.5:5672
Channel: axis_changes
============================================================
```

**Keep this running** and proceed to the next step.

---

## Step 9: Trigger Changes and Verify

### In another terminal/PowerShell window:

```powershell
# Connect to source database
psql -h 192.168.29.5 -U postgres -d source_db -p 5432
```

### Run test queries:

```sql
-- Insert new user
INSERT INTO users (email, name, phone) 
VALUES ('test@example.com', 'Test User', '555-0000');

-- Update existing user
UPDATE users SET name = 'John Updated' WHERE email = 'john@example.com';

-- Insert order
INSERT INTO orders (user_id, product_name, quantity, price)
VALUES (1, 'Test Product', 2, 99.99);

-- Delete user
DELETE FROM users WHERE email = 'test@example.com';
```

### Check Axis logs
You should see messages being published to RabbitMQ in the Axis terminal.

---

## Step 10: Run Axis - Test 3 (Full Replication)

Stop the simple mode (Ctrl+C) and run full orchestrator:

```powershell
python src/cli.py --replicas "postgresql://postgres:postgres@192.168.29.5:5433/replica_db"
```

Expected output:
```
============================================================
AXIS - Async PostgreSQL Replication System
============================================================
Mode: Full Orchestrator
Source DB: 192.168.29.5:5432/source_db
RabbitMQ: 192.168.29.5:5672
Channel: axis_changes
============================================================
Starting Axis replication service...
Configured 1 replica(s)
Connected to source database
Setup replication for tables: ['users', 'orders', 'products']
Started RabbitMQ publisher
Started replica consumer 0
✓ Replication orchestrator started successfully
HTTP server started on http://0.0.0.0:8080
```

---

## Step 11: Verify Replication

### Terminal 1: Make changes in source database

```sql
-- Connect to source
psql -h 192.168.29.5 -U postgres -d source_db -p 5432

-- Insert data
INSERT INTO users (email, name, phone) 
VALUES ('alice@example.com', 'Alice Wonder', '555-1111');

INSERT INTO products (name, description, price, stock)
VALUES ('Monitor', '27-inch 4K monitor', 399.99, 30);
```

### Terminal 2: Check replica database

```sql
-- Connect to replica
psql -h 192.168.29.5 -U postgres -d replica_db -p 5433

-- Verify data was replicated
SELECT * FROM users WHERE email = 'alice@example.com';
SELECT * FROM products WHERE name = 'Monitor';

-- Should see the same data as source!
```

---

## Step 12: Test Monitoring Endpoints

Open your browser and visit:

1. **Health Check:** http://localhost:8080/health
2. **Readiness:** http://localhost:8080/ready
3. **Metrics:** http://localhost:8080/metrics
4. **Status:** http://localhost:8080/status

---

## Test Scenarios

### Test 1: Basic INSERT Replication
```sql
-- Source DB
INSERT INTO users (email, name, phone) VALUES ('user1@test.com', 'User One', '555-0001');

-- Replica DB (should have same data)
SELECT * FROM users WHERE email = 'user1@test.com';
```

### Test 2: UPDATE Replication
```sql
-- Source DB
UPDATE users SET name = 'User One Updated' WHERE email = 'user1@test.com';

-- Replica DB (should see updated name)
SELECT * FROM users WHERE email = 'user1@test.com';
```

### Test 3: DELETE Replication
```sql
-- Source DB
DELETE FROM users WHERE email = 'user1@test.com';

-- Replica DB (user should be deleted)
SELECT * FROM users WHERE email = 'user1@test.com';  -- Should return 0 rows
```

### Test 4: Bulk Operations
```sql
-- Source DB
INSERT INTO orders (user_id, product_name, quantity, price)
SELECT 
    u.id,
    p.name,
    (RANDOM() * 5 + 1)::INTEGER,
    p.price
FROM users u
CROSS JOIN products p
LIMIT 20;

-- Replica DB (should have all orders)
SELECT COUNT(*) FROM orders;  -- Should match source
```

### Test 5: High-Volume Test
```sql
-- Source DB
DO $$
BEGIN
    FOR i IN 1..100 LOOP
        INSERT INTO users (email, name, phone) 
        VALUES ('bulk' || i || '@test.com', 'Bulk User ' || i, '555-' || LPAD(i::TEXT, 4, '0'));
    END LOOP;
END $$;

-- Check Axis metrics
-- Visit http://localhost:8080/metrics
-- Look for axis_events_received_total and axis_events_published_total
```

---

## Troubleshooting

### Issue: Cannot connect to PostgreSQL

```bash
# Check if PostgreSQL is running on 192.168.29.5
ssh user@192.168.29.5
docker ps | grep postgres

# Check if ports are accessible
# From your local machine:
telnet 192.168.29.5 5432
telnet 192.168.29.5 5433
```

### Issue: Cannot connect to RabbitMQ

```bash
# Check RabbitMQ status
docker ps | grep rabbitmq

# Test connection
telnet 192.168.29.5 5672
```

### Issue: Triggers not working

The triggers are created automatically by Axis. Check if they exist:

```sql
-- On source database
SELECT * FROM pg_trigger WHERE tgname LIKE 'axis_%';
```

### Issue: Replication lag

Check the metrics:
```bash
curl http://localhost:8080/metrics | grep axis_replication_lag
```

---

## Expected Results

✅ **Success Indicators:**
- Axis starts without errors
- HTTP server responds on port 8080
- Changes in source DB appear in replica DB within seconds
- Metrics show events being processed
- No errors in Axis logs

📊 **Performance Expectations:**
- Replication lag: < 1 second for single events
- Throughput: 100+ events/second
- Memory usage: < 200MB for normal workload

---

## Next Steps After Testing

1. Test with PII masking (see examples/example_with_transformer.py)
2. Test message routing (see examples/example_with_dispatcher.py)
3. Set up Prometheus monitoring
4. Configure Grafana dashboards
5. Test failover scenarios
6. Performance benchmarking

---

## Quick Reference

### Connection Strings
```
Source DB:   postgresql://postgres:postgres@192.168.29.5:5432/source_db
Replica DB:  postgresql://postgres:postgres@192.168.29.5:5433/replica_db
RabbitMQ:    amqp://guest:guest@192.168.29.5:5672/
```

### Ports Used
- **5432** - PostgreSQL Source
- **5433** - PostgreSQL Replica
- **5672** - RabbitMQ AMQP
- **15672** - RabbitMQ Management UI
- **8080** - Axis HTTP Monitoring

### Useful Commands
```powershell
# Start Axis with monitoring
python src/cli.py --replicas "postgresql://postgres:postgres@192.168.29.5:5433/replica_db"

# Start without HTTP server
python src/cli.py --no-http --replicas "postgresql://postgres:postgres@192.168.29.5:5433/replica_db"

# Custom HTTP port
python src/cli.py --http-port 9090 --replicas "postgresql://postgres:postgres@192.168.29.5:5433/replica_db"
```
