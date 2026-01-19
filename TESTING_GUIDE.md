# 🧪 Replication Testing Guide

This guide explains how to test the Axis replication system with your PostgreSQL databases.

## Prerequisites

✅ PostgreSQL source database running on 192.168.29.5:5432  
✅ PostgreSQL replica database running on 192.168.29.5:5433  
✅ RabbitMQ running on 192.168.29.5:5672  
✅ Test data already inserted (3 users, 3 products, 3 orders)

## Quick Start

### Option 1: Automated Testing (Recommended)

**Step 1:** Start Axis in a dedicated terminal

Open a NEW PowerShell window and run:
```powershell
cd C:\Users\saipr\Projects\Axis
.\start_axis.bat
```

You should see:
```
============================================================
AXIS - Async PostgreSQL Replication System
============================================================
Mode: Full Orchestrator
Source DB: 192.168.29.5:5432/source_db
RabbitMQ: amqp://guest:guest@192.168.29.5:5672/
HTTP Server: http://localhost:8080
  - Health: http://localhost:8080/health
  - Metrics: http://localhost:8080/metrics
✓ Replication orchestrator started successfully
Listening on channel: axis_channel
```

**Step 2:** Run the automated test in your current terminal

```powershell
.\venv\Scripts\python.exe tests\test_replication.py
```

The test will:
1. ✓ INSERT two test users
2. ✓ UPDATE one user's name
3. ✓ DELETE one user
4. ✓ Verify row counts match after each operation

**Expected Output:**
```
======================================================================
 AXIS REPLICATION TEST
======================================================================

TEST 1: INSERT Operation
✓ Inserted user: Test User 1 (test1@example.com)
Waiting 3 seconds for replication...
  Source DB users: 4
  Replica DB users: 4
  ✅ PASS - Row counts match

TEST 2: Another INSERT
✓ Inserted user: Test User 2 (test2@example.com)
Waiting 3 seconds for replication...
  Source DB users: 5
  Replica DB users: 5
  ✅ PASS - Row counts match

TEST 3: UPDATE Operation
✓ Updated user test1@example.com -> Updated User 1
Waiting 3 seconds for replication...
  Source name: Updated User 1
  Replica name: Updated User 1
  ✅ PASS - UPDATE replicated correctly

TEST 4: DELETE Operation
✓ Deleted user: test2@example.com
Waiting 3 seconds for replication...
  Source DB users: 4
  Replica DB users: 4
  ✅ PASS - Row counts match

======================================================================
 TEST SUMMARY
======================================================================
  Tests Passed: 4/4
  Tests Failed: 0/4
======================================================================

🎉 ALL TESTS PASSED! Replication is working correctly.
```

### Option 2: Manual Testing

Use [manual_test_guide.bat](tests/manual_test_guide.bat) for step-by-step instructions to test manually with SQL commands.

## Monitoring Endpoints

While Axis is running, check these URLs:

- **Health Check**: http://localhost:8080/health
- **Readiness**: http://localhost:8080/ready
- **Prometheus Metrics**: http://localhost:8080/metrics
- **Status**: http://localhost:8080/status

### Example: Check Health

```powershell
curl http://localhost:8080/health
```

Response:
```json
{
  "status": "healthy",
  "timestamp": "2025-12-15T00:10:55.037993",
  "uptime_seconds": 26.26
}
```

### Example: Check Metrics

```powershell
curl http://localhost:8080/metrics
```

You'll see Prometheus metrics like:
```
# HELP axis_events_received_total Total events received from source database
# TYPE axis_events_received_total counter
axis_events_received_total{table="users"} 5.0

# HELP axis_events_replicated_total Total events successfully replicated
# TYPE axis_events_replicated_total counter
axis_events_replicated_total{replica="0"} 5.0

# HELP axis_replication_lag_seconds Current replication lag
# TYPE axis_replication_lag_seconds gauge
axis_replication_lag_seconds{replica="0"} 0.023
```

## Troubleshooting

### Axis Won't Start

**Problem**: `ModuleNotFoundError: No module named 'config'`

**Solution**: Make sure PYTHONPATH is set. Use the provided `start_axis.bat` script.

---

**Problem**: `ValueError: PG_DSN environment variable is required`

**Solution**: Check that `.env` file exists in the project root with correct values.

---

### Replication Not Working

1. **Check Axis logs** in the window where it's running
2. **Verify connectivity**:
   ```powershell
   .\venv\Scripts\python.exe tests\test_connectivity.py
   ```
3. **Check RabbitMQ**: Visit http://192.168.29.5:15672 (guest/guest)
4. **Check triggers**: Verify triggers exist on source database tables

---

### Test Fails with Connection Error

Make sure:
- Both PostgreSQL instances are running
- Firewall allows connections to 192.168.29.5
- Passwords are correct (default: "password")

## Next Steps

After successful testing:

1. **Performance Testing**: Insert bulk data and measure replication lag
2. **Stress Testing**: Run multiple concurrent operations
3. **Failover Testing**: Stop/restart components to test resilience
4. **Monitoring**: Set up Prometheus to scrape metrics endpoint

## Architecture

```
┌─────────────────┐         ┌──────────────┐         ┌─────────────────┐
│  PostgreSQL     │         │   RabbitMQ   │         │  PostgreSQL     │
│  Source         │────────▶│   Exchange   │────────▶│  Replica        │
│  :5432          │ NOTIFY  │   :5672      │ Consume │  :5433          │
└─────────────────┘         └──────────────┘         └─────────────────┘
        │                           │                          │
        └───────────────┬───────────┴──────────────────────────┘
                        │
                ┌───────▼────────┐
                │  Axis System   │
                │                │
                │  • Listener    │
                │  • Publisher   │
                │  • Consumer    │
                │  • Monitoring  │
                └────────────────┘
                        │
                        │ HTTP :8080
                        ▼
                ┌───────────────┐
                │  Prometheus   │
                │  Metrics      │
                └───────────────┘
```

## Files

- `start_axis.bat` - Startup script for Axis
- `tests/test_replication.py` - Automated replication test
- `tests/test_connectivity.py` - Database connectivity test  
- `tests/insert_test_data.py` - Insert initial test data
- `tests/manual_test_guide.bat` - Manual testing guide
- `.env` - Configuration file

---

**Ready to test?** Follow Option 1 above to start! 🚀
