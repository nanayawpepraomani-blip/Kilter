# Kilter — MySQL Deployment Guide

## Why MySQL?

Kilter defaults to SQLite (zero configuration, ideal for pilots). For production banks with multiple simultaneous operators, MySQL provides:

- Multi-writer concurrency (no SQLite WAL bottleneck)
- Centralized storage (multiple app instances can share one DB)
- Enterprise backup tooling (mysqldump, Percona XtraBackup)
- Native replication for HA

## Requirements

- MySQL 8.0+ (or MariaDB 10.6+)
- `mysql-connector-python` Python package

## Setup

### 1. Create database and user

```sql
CREATE DATABASE kilter CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER 'kilter'@'%' IDENTIFIED BY 'strong-random-password';
GRANT ALL PRIVILEGES ON kilter.* TO 'kilter'@'%';
FLUSH PRIVILEGES;
```

### 2. Install the Python driver

```bash
.venv/bin/pip install mysql-connector-python
```

Or add to requirements.txt (uncomment the line).

### 3. Set the connection string

In your `.env` file:
```
DATABASE_URL=mysql://kilter:strong-random-password@db-host:3306/kilter
```

### 4. First run

```bash
.venv/bin/uvicorn app:app --host 127.0.0.1 --port 8000
```

Schema is created automatically on startup.

## Migrating from SQLite

1. Export from SQLite:
```bash
sqlite3 kilter.db .dump > kilter_export.sql
```

2. The SQL will need manual adjustment for MySQL syntax (AUTOINCREMENT → AUTO_INCREMENT, etc.). For large databases, use a dedicated tool like [pgloader](https://pgloader.io/) configured for MySQL output, or the `sqlite3-to-mysql` Python package:

```bash
pip install sqlite3-to-mysql
sqlite3mysql -f kilter.db -d kilter -u kilter --mysql-password yourpassword -h db-host
```

3. After migration, set `DATABASE_URL` and restart.

## Connection Pooling

For high-concurrency deployments, put [ProxySQL](https://www.proxysql.com/) or MySQL Router in front:

```
App → ProxySQL (connection pool) → MySQL primary
                                 → MySQL replica (read scaling)
```

## Backup

```bash
# Logical backup (safe while app is running)
mysqldump --single-transaction --routines --triggers \
  -u kilter -p kilter > kilter_$(date +%Y%m%d_%H%M).sql

# Point-in-time recovery with binlog
# Enable in /etc/mysql/mysql.conf.d/mysqld.cnf:
# log_bin = /var/log/mysql/mysql-bin.log
# expire_logs_days = 14
```
