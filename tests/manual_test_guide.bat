@echo off
echo ============================================================
echo AXIS REPLICATION MANUAL TEST
echo ============================================================
echo.
echo Instructions:
echo 1. Keep Axis running in a separate window
echo 2. Run these SQL commands manually using psql or pgAdmin
echo 3. Verify results in both source and replica databases
echo.
echo ============================================================
echo TEST 1: Initial Row Count
echo ============================================================
echo.
echo Source Database (port 5432):
echo   SELECT COUNT(*) FROM users;
echo.
echo Replica Database (port 5433):
echo   SELECT COUNT(*) FROM users;
echo.
pause
echo.
echo ============================================================
echo TEST 2: INSERT Operation
echo ============================================================
echo.
echo Run on SOURCE database (port 5432):
echo   INSERT INTO users (name, email) VALUES ('Test User 1', 'test1@example.com');
echo.
echo Wait 2-3 seconds, then check REPLICA (port 5433):
echo   SELECT * FROM users WHERE email = 'test1@example.com';
echo.
echo Expected: Row should exist in replica
echo.
pause
echo.
echo ============================================================
echo TEST 3: Another INSERT
echo ============================================================
echo.
echo Run on SOURCE:
echo   INSERT INTO users (name, email) VALUES ('Test User 2', 'test2@example.com');
echo.
echo Check REPLICA:
echo   SELECT COUNT(*) FROM users;
echo.
pause
echo.
echo ============================================================
echo TEST 4: UPDATE Operation
echo ============================================================
echo.
echo Run on SOURCE:
echo   UPDATE users SET name = 'Updated User 1' WHERE email = 'test1@example.com';
echo.
echo Check REPLICA:
echo   SELECT name FROM users WHERE email = 'test1@example.com';
echo.
echo Expected: name should be 'Updated User 1'
echo.
pause
echo.
echo ============================================================
echo TEST 5: DELETE Operation
echo ============================================================
echo.
echo Run on SOURCE:
echo   DELETE FROM users WHERE email = 'test2@example.com';
echo.
echo Check REPLICA:
echo   SELECT * FROM users WHERE email = 'test2@example.com';
echo.
echo Expected: No rows returned
echo.
pause
echo.
echo ============================================================
echo TESTING COMPLETE
echo ============================================================
echo.
echo Check the Axis logs for replication events.
echo Monitor metrics at: http://localhost:8080/metrics
echo.
pause
