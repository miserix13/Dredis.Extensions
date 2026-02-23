# Dredis.Extensions.Storage.SqlServer

SQL Server-backed implementation of `IKeyValueStore` for Dredis.

## Current support

- String key/value operations: `GET`, `SET`, `MGET`, `MSET`, `DEL`, `EXISTS`, `INCRBY`
- Expiration operations: `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`
- Expired-key cleanup: `CleanUpExpiredKeysAsync`

## Notes

- Table is created automatically on first use (default: `dbo.DredisKeyValue`).
- Advanced Redis data structures currently throw `NotSupportedException` in this extension.
- Constructor:
  - `SqlServerKeyValueStore(string connectionString, string tableName = "DredisKeyValue")`
