#!/usr/bin/env bash
#
# Provisions a two-node SQL Server Always On availability group (CLUSTER_TYPE = NONE,
# no WSFC/Pacemaker needed) running in Docker, with CDC enabled on the primary and
# a readable secondary you can query directly.
#
# Usage: ./scripts/provision.sh
#
# Safe to re-run individual sections manually via run_sql() if a step fails partway
# through -- but re-running the whole script from scratch requires `docker compose down -v`
# first, since certificates/logins/AG state will already exist otherwise.

set -euo pipefail

SA_PASSWORD="YourStrong!Passw0rd"
MASTER_KEY_PW="MasterKeyPassw0rd!1"
AG_NAME="ag1"
DB_NAME="testdb"
NODE1="mssql-node1"
NODE2="mssql-node2"
SQLCMD="/opt/mssql-tools18/bin/sqlcmd -C"

cd "$(dirname "$0")/.."

run_sql() {
  # Usage: run_sql <container> <<SQL ... SQL
  local container=$1
  docker exec -i "$container" $SQLCMD -b -S localhost -U sa -P "$SA_PASSWORD"
}

query_scalar() {
  # Usage: query_scalar <container> "<T-SQL returning one value>"
  local container=$1
  local sql=$2
  docker exec "$container" $SQLCMD -S localhost -U sa -P "$SA_PASSWORD" -h -1 -W -Q "SET NOCOUNT ON; $sql"
}

wait_for_sql() {
  local container=$1
  echo "Waiting for $container to accept connections..."
  until docker exec "$container" $SQLCMD -S localhost -U sa -P "$SA_PASSWORD" -Q "SELECT 1" &>/dev/null; do
    sleep 2
  done
  echo "$container is ready."
}

echo "== Starting containers =="
docker compose up -d

wait_for_sql "$NODE1"
wait_for_sql "$NODE2"

echo "== Creating master keys, certificates, and HADR endpoints =="

run_sql "$NODE1" <<SQL
CREATE MASTER KEY ENCRYPTION BY PASSWORD = '${MASTER_KEY_PW}';
GO
CREATE CERTIFICATE node1_cert
WITH SUBJECT = 'node1 AG certificate';
GO
BACKUP CERTIFICATE node1_cert
TO FILE = '/var/opt/mssql/data/node1_cert.cer';
GO
CREATE ENDPOINT hadr_endpoint
STATE = STARTED
AS TCP (LISTENER_PORT = 5022, LISTENER_IP = ALL)
FOR DATABASE_MIRRORING (AUTHENTICATION = CERTIFICATE node1_cert, ROLE = ALL);
GO
SQL

run_sql "$NODE2" <<SQL
CREATE MASTER KEY ENCRYPTION BY PASSWORD = '${MASTER_KEY_PW}';
GO
CREATE CERTIFICATE node2_cert
WITH SUBJECT = 'node2 AG certificate';
GO
BACKUP CERTIFICATE node2_cert
TO FILE = '/var/opt/mssql/data/node2_cert.cer';
GO
CREATE ENDPOINT hadr_endpoint
STATE = STARTED
AS TCP (LISTENER_PORT = 5022, LISTENER_IP = ALL)
FOR DATABASE_MIRRORING (AUTHENTICATION = CERTIFICATE node2_cert, ROLE = ALL);
GO
SQL

echo "== Exchanging certificates between nodes =="
mkdir -p certs
docker cp "$NODE1":/var/opt/mssql/data/node1_cert.cer ./certs/node1_cert.cer
docker cp "$NODE2":/var/opt/mssql/data/node2_cert.cer ./certs/node2_cert.cer
docker cp ./certs/node1_cert.cer "$NODE2":/var/opt/mssql/data/node1_cert.cer
docker cp ./certs/node2_cert.cer "$NODE1":/var/opt/mssql/data/node2_cert.cer
# docker cp writes files as root; SQL Server runs as mssql and must be able to read them
docker exec -u root "$NODE1" chown mssql:root /var/opt/mssql/data/node2_cert.cer
docker exec -u root "$NODE2" chown mssql:root /var/opt/mssql/data/node1_cert.cer

echo "== Restoring peer certificates and granting endpoint CONNECT =="

run_sql "$NODE1" <<SQL
CREATE LOGIN node2_login WITH PASSWORD = '${MASTER_KEY_PW}';
CREATE USER node2_user FOR LOGIN node2_login;
GO
CREATE CERTIFICATE node2_cert
    AUTHORIZATION node2_user
    FROM FILE = '/var/opt/mssql/data/node2_cert.cer';
GO
GRANT CONNECT ON ENDPOINT::hadr_endpoint TO node2_login;
GO
SQL

run_sql "$NODE2" <<SQL
CREATE LOGIN node1_login WITH PASSWORD = '${MASTER_KEY_PW}';
CREATE USER node1_user FOR LOGIN node1_login;
GO
CREATE CERTIFICATE node1_cert
    AUTHORIZATION node1_user
    FROM FILE = '/var/opt/mssql/data/node1_cert.cer';
GO
GRANT CONNECT ON ENDPOINT::hadr_endpoint TO node1_login;
GO
SQL

echo "== Creating and backing up ${DB_NAME} on ${NODE1} (required before it can join an AG) =="

run_sql "$NODE1" <<SQL
CREATE DATABASE ${DB_NAME};
GO
ALTER DATABASE ${DB_NAME} SET RECOVERY FULL;
GO
BACKUP DATABASE ${DB_NAME} TO DISK = '/var/opt/mssql/data/${DB_NAME}.bak';
GO
SQL

echo "== Creating availability group ${AG_NAME} (CLUSTER_TYPE = NONE) on ${NODE1} =="

run_sql "$NODE1" <<SQL
CREATE AVAILABILITY GROUP [${AG_NAME}]
WITH (CLUSTER_TYPE = NONE)
FOR DATABASE [${DB_NAME}]
REPLICA ON
  N'${NODE1}' WITH (
    ENDPOINT_URL = N'TCP://${NODE1}:5022',
    FAILOVER_MODE = MANUAL,
    AVAILABILITY_MODE = SYNCHRONOUS_COMMIT,
    SECONDARY_ROLE(ALLOW_CONNECTIONS = ALL)
  ),
  N'${NODE2}' WITH (
    ENDPOINT_URL = N'TCP://${NODE2}:5022',
    FAILOVER_MODE = MANUAL,
    AVAILABILITY_MODE = SYNCHRONOUS_COMMIT,
    SEEDING_MODE = AUTOMATIC,
    SECONDARY_ROLE(ALLOW_CONNECTIONS = ALL)
  );
GO
SQL

echo "== Joining ${NODE2} to ${AG_NAME} and enabling automatic seeding =="

run_sql "$NODE2" <<SQL
ALTER AVAILABILITY GROUP [${AG_NAME}] JOIN WITH (CLUSTER_TYPE = NONE);
GO
ALTER AVAILABILITY GROUP [${AG_NAME}] GRANT CREATE ANY DATABASE;
GO
SQL

echo "== Waiting for automatic seeding to bring ${DB_NAME} online on ${NODE2} =="
for i in $(seq 1 60); do
  state=$(query_scalar "$NODE2" "SELECT state_desc FROM sys.databases WHERE name = '${DB_NAME}'" | tr -d '[:space:]')
  if [ "$state" == "ONLINE" ]; then
    echo "${DB_NAME} is ONLINE on ${NODE2}."
    break
  fi
  echo "  ...current state: '${state:-<not yet present>}' (attempt $i/60)"
  sleep 3
done

echo "== Enabling CDC on ${NODE1} (primary) and creating the test table =="

run_sql "$NODE1" <<SQL
USE ${DB_NAME};
EXEC sys.sp_cdc_enable_db;
GO
CREATE TABLE dbo.orders (
    id INT IDENTITY PRIMARY KEY,
    payload NVARCHAR(100),
    inserted_at DATETIME2 DEFAULT SYSUTCDATETIME()
);
GO
EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name   = N'orders',
    @role_name     = NULL,
    @supports_net_changes = 1;
GO
SQL

echo ""
echo "== Done =="
echo "Primary   (${NODE1}): localhost,14331"
echo "Secondary (${NODE2}): localhost,14332"
echo ""
echo "Sanity checks you can run now:"
echo "  docker exec -it ${NODE1} ${SQLCMD} -S localhost -U sa -P '${SA_PASSWORD}' -Q \"SELECT * FROM sys.dm_hadr_database_replica_states\""
echo "  docker exec -it ${NODE2} ${SQLCMD} -S localhost -U sa -P '${SA_PASSWORD}' -Q \"SELECT sys.fn_cdc_get_max_lsn()\""
echo ""
echo "Next: run the Python harness in ./python against localhost,14331 and localhost,14332."
