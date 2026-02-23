using System.Data;
using System.Globalization;
using System.Text;
using Dredis.Abstractions.Storage;
using Microsoft.Data.SqlClient;

namespace Dredis.Extensions.Storage.SqlServer;

public sealed partial class SqlServerKeyValueStore : IKeyValueStore
{
    private readonly string _connectionString;
    private readonly string _tableName;
    private readonly SemaphoreSlim _schemaLock = new(1, 1);
    private volatile bool _schemaInitialized;

    public SqlServerKeyValueStore(string connectionString, string tableName = "DredisKeyValue")
    {
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new ArgumentException("Connection string is required.", nameof(connectionString));
        }

        if (string.IsNullOrWhiteSpace(tableName))
        {
            throw new ArgumentException("Table name is required.", nameof(tableName));
        }

        _connectionString = connectionString;
        _tableName = tableName;
    }

    public Task<byte[]?> GetAsync(string key, CancellationToken token = default)
    {
        return GetCoreAsync(key, token);
    }

    public Task<bool> SetAsync(
                string key,
                byte[] value,
                TimeSpan? expiration,
                SetCondition condition,
                CancellationToken token = default)
    {
        return SetCoreAsync(key, value, expiration, condition, token);
    }

    public Task<byte[]?[]> GetManyAsync(string[] keys, CancellationToken token = default)
    {
        return GetManyCoreAsync(keys, token);
    }

    public Task<bool> SetManyAsync(KeyValuePair<string, byte[]>[] items, CancellationToken token = default)
    {
        return SetManyCoreAsync(items, token);
    }

    public Task<long> DeleteAsync(string[] keys, CancellationToken token = default)
    {
        return DeleteCoreAsync(keys, token);
    }

    public Task<bool> ExistsAsync(string key, CancellationToken token = default)
    {
        return ExistsCoreAsync(key, token);
    }

    public Task<long> ExistsAsync(string[] keys, CancellationToken token = default)
    {
        return ExistsManyCoreAsync(keys, token);
    }

    public Task<long?> IncrByAsync(string key, long delta, CancellationToken token = default)
    {
        return IncrByCoreAsync(key, delta, token);
    }

    public Task<bool> ExpireAsync(string key, TimeSpan expiration, CancellationToken token = default)
    {
        return ExpireCoreAsync(key, expiration, token);
    }

    public Task<bool> PExpireAsync(string key, TimeSpan expiration, CancellationToken token = default)
    {
        return ExpireCoreAsync(key, expiration, token);
    }

    public Task<long> TtlAsync(string key, CancellationToken token = default)
    {
        return TtlCoreAsync(key, milliseconds: false, token);
    }

    public Task<long> PttlAsync(string key, CancellationToken token = default)
    {
        return TtlCoreAsync(key, milliseconds: true, token);
    }

    public Task<bool> HashSetAsync(string key, string field, byte[] value, CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<byte[]?> HashGetAsync(string key, string field, CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<long> HashDeleteAsync(string key, string[] fields, CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<KeyValuePair<string, byte[]>[]> HashGetAllAsync(string key, CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<ListPushResult> ListPushAsync(
                string key,
                byte[][] values,
                bool left,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<ListPopResult> ListPopAsync(
                string key,
                bool left,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<ListRangeResult> ListRangeAsync(
                string key,
                int start,
                int stop,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<ListLengthResult> ListLengthAsync(
                string key,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<ListIndexResult> ListIndexAsync(
                string key,
                int index,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<ListSetResult> ListSetAsync(
                string key,
                int index,
                byte[] value,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<ListResultStatus> ListTrimAsync(
                string key,
                int start,
                int stop,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<SetCountResult> SetAddAsync(
                string key,
                byte[][] members,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<SetCountResult> SetRemoveAsync(
                string key,
                byte[][] members,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<SetMembersResult> SetMembersAsync(
                string key,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<SetCountResult> SetCardinalityAsync(
                string key,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<SortedSetCountResult> SortedSetAddAsync(
                string key,
                SortedSetEntry[] entries,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<SortedSetCountResult> SortedSetRemoveAsync(
                string key,
                byte[][] members,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<SortedSetRangeResult> SortedSetRangeAsync(
                string key,
                int start,
                int stop,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<SortedSetCountResult> SortedSetCardinalityAsync(
                string key,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<SortedSetRangeResult> SortedSetRangeByScoreAsync(
                string key,
                double minScore,
                double maxScore,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<SortedSetScoreResult> SortedSetScoreAsync(
                string key,
                byte[] member,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<SortedSetScoreResult> SortedSetIncrementAsync(
                string key,
                double increment,
                byte[] member,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<SortedSetCountResult> SortedSetCountByScoreAsync(
                string key,
                double minScore,
                double maxScore,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<SortedSetRankResult> SortedSetRankAsync(
                string key,
                byte[] member,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<SortedSetRankResult> SortedSetReverseRankAsync(
                string key,
                byte[] member,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<SortedSetRemoveRangeResult> SortedSetRemoveRangeByScoreAsync(
                string key,
                double minScore,
                double maxScore,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<string?> StreamAddAsync(
                string key,
                string id,
                KeyValuePair<string, byte[]>[] fields,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<long> StreamDeleteAsync(string key, string[] ids, CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<long> StreamLengthAsync(string key, CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<string?> StreamLastIdAsync(string key, CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<StreamReadResult[]> StreamReadAsync(
                string[] keys,
                string[] ids,
                int? count,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<StreamEntry[]> StreamRangeAsync(
                string key,
                string start,
                string end,
                int? count,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<StreamEntry[]> StreamRangeReverseAsync(
                string key,
                string start,
                string end,
                int? count,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<long> StreamTrimAsync(
                string key,
                int? maxLength = null,
                string? minId = null,
                bool approximate = false,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<StreamInfoResult> StreamInfoAsync(string key, CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<StreamGroupsInfoResult> StreamGroupsInfoAsync(string key, CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<StreamConsumersInfoResult> StreamConsumersInfoAsync(
                string key,
                string group,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<StreamSetIdResultStatus> StreamSetIdAsync(
                string key,
                string lastId,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<StreamGroupCreateResult> StreamGroupCreateAsync(
                string key,
                string group,
                string startId,
                bool mkStream,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<StreamGroupDestroyResult> StreamGroupDestroyAsync(
                string key,
                string group,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<StreamGroupSetIdResultStatus> StreamGroupSetIdAsync(
                string key,
                string group,
                string lastId,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<StreamGroupDelConsumerResult> StreamGroupDelConsumerAsync(
                string key,
                string group,
                string consumer,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<StreamGroupReadResult> StreamGroupReadAsync(
                string group,
                string consumer,
                string[] keys,
                string[] ids,
                int? count,
                TimeSpan? block,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<StreamAckResult> StreamAckAsync(
                string key,
                string group,
                string[] ids,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<StreamPendingResult> StreamPendingAsync(
                string key,
                string group,
                long? minIdleTimeMs = null,
                string? start = null,
                string? end = null,
                int? count = null,
                string? consumer = null,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<StreamClaimResult> StreamClaimAsync(
                string key,
                string group,
                string consumer,
                long minIdleTimeMs,
                string[] ids,
                long? idleMs = null,
                long? timeMs = null,
                long? retryCount = null,
                bool force = false,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<long> CleanUpExpiredKeysAsync(CancellationToken token = default)
    {
        return CleanUpExpiredKeysCoreAsync(token);
    }

    public Task<JsonSetResult> JsonSetAsync(
                string key,
                string path,
                byte[] value,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<JsonGetResult> JsonGetAsync(
                string key,
                string[] paths,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<JsonDelResult> JsonDelAsync(
                string key,
                string[] paths,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<JsonTypeResult> JsonTypeAsync(
                string key,
                string[] paths,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<JsonArrayResult> JsonStrlenAsync(
                string key,
                string[] paths,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<JsonArrayResult> JsonArrlenAsync(
                string key,
                string[] paths,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<JsonArrayResult> JsonArrappendAsync(
                string key,
                string path,
                byte[][] values,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<JsonGetResult> JsonArrindexAsync(
                string key,
                string path,
                byte[] value,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<JsonArrayResult> JsonArrinsertAsync(
                string key,
                string path,
                int index,
                byte[][] values,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<JsonArrayResult> JsonArrremAsync(
                string key,
                string path,
                int? index,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<JsonArrayResult> JsonArrtrimAsync(
                string key,
                string path,
                int start,
                int stop,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<JsonMGetResult> JsonMgetAsync(
                string[] keys,
                string path,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<HyperLogLogAddResult> HyperLogLogAddAsync(
                string key,
                byte[][] elements,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<HyperLogLogCountResult> HyperLogLogCountAsync(
                string[] keys,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<HyperLogLogMergeResult> HyperLogLogMergeAsync(
                string destinationKey,
                string[] sourceKeys,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<ProbabilisticResultStatus> BloomReserveAsync(
                string key,
                double errorRate,
                long capacity,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<ProbabilisticBoolResult> BloomAddAsync(
                string key,
                byte[] element,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<ProbabilisticArrayResult> BloomMAddAsync(
                string key,
                byte[][] elements,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<ProbabilisticBoolResult> BloomExistsAsync(
                string key,
                byte[] element,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<ProbabilisticArrayResult> BloomMExistsAsync(
                string key,
                byte[][] elements,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<ProbabilisticInfoResult> BloomInfoAsync(
                string key,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<ProbabilisticResultStatus> CuckooReserveAsync(
                string key,
                long capacity,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<ProbabilisticBoolResult> CuckooAddAsync(
                string key,
                byte[] item,
                bool noCreate,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<ProbabilisticBoolResult> CuckooAddNxAsync(
                string key,
                byte[] item,
                bool noCreate,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<ProbabilisticBoolResult> CuckooExistsAsync(
                string key,
                byte[] item,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<ProbabilisticBoolResult> CuckooDeleteAsync(
                string key,
                byte[] item,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<ProbabilisticCountResult> CuckooCountAsync(
                string key,
                byte[] item,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<ProbabilisticInfoResult> CuckooInfoAsync(
                string key,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<ProbabilisticResultStatus> TDigestCreateAsync(
                string key,
                int compression,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<ProbabilisticResultStatus> TDigestResetAsync(
                string key,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<ProbabilisticResultStatus> TDigestAddAsync(
                string key,
                double[] values,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<ProbabilisticDoubleArrayResult> TDigestQuantileAsync(
                string key,
                double[] quantiles,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<ProbabilisticDoubleArrayResult> TDigestCdfAsync(
                string key,
                double[] values,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<ProbabilisticArrayResult> TDigestRankAsync(
                string key,
                double[] values,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<ProbabilisticArrayResult> TDigestRevRankAsync(
                string key,
                double[] values,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<ProbabilisticDoubleArrayResult> TDigestByRankAsync(
                string key,
                long[] ranks,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<ProbabilisticDoubleArrayResult> TDigestByRevRankAsync(
                string key,
                long[] ranks,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<ProbabilisticDoubleResult> TDigestTrimmedMeanAsync(
                string key,
                double lowerQuantile,
                double upperQuantile,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<ProbabilisticDoubleResult> TDigestMinAsync(
                string key,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<ProbabilisticDoubleResult> TDigestMaxAsync(
                string key,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<ProbabilisticInfoResult> TDigestInfoAsync(
                string key,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<ProbabilisticResultStatus> TopKReserveAsync(
                string key,
                int k,
                int width,
                int depth,
                double decay,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<ProbabilisticStringArrayResult> TopKAddAsync(
                string key,
                byte[][] items,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<ProbabilisticStringArrayResult> TopKIncrByAsync(
                string key,
                KeyValuePair<byte[], long>[] increments,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<ProbabilisticArrayResult> TopKQueryAsync(
                string key,
                byte[][] items,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<ProbabilisticArrayResult> TopKCountAsync(
                string key,
                byte[][] items,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<ProbabilisticStringArrayResult> TopKListAsync(
                string key,
                bool withCount,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<ProbabilisticInfoResult> TopKInfoAsync(
                string key,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<VectorSetResult> VectorSetAsync(
                string key,
                double[] vector,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<VectorGetResult> VectorGetAsync(
                string key,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<VectorSizeResult> VectorSizeAsync(
                string key,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<VectorSimilarityResult> VectorSimilarityAsync(
                string key,
                string otherKey,
                string metric,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<VectorDeleteResult> VectorDeleteAsync(
                string key,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<VectorSearchResult> VectorSearchAsync(
                string keyPrefix,
                int topK,
                int offset,
                string metric,
                double[] queryVector,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<TimeSeriesResultStatus> TimeSeriesCreateAsync(
                string key,
                long? retentionTimeMs,
                TimeSeriesDuplicatePolicy? duplicatePolicy,
                KeyValuePair<string, string>[]? labels,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<TimeSeriesAddResult> TimeSeriesAddAsync(
                string key,
                long timestamp,
                double value,
                TimeSeriesDuplicatePolicy? onDuplicate,
                bool createIfMissing,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<TimeSeriesAddResult> TimeSeriesIncrementByAsync(
                string key,
                double increment,
                long? timestamp,
                bool createIfMissing,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<TimeSeriesGetResult> TimeSeriesGetAsync(
                string key,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<TimeSeriesRangeResult> TimeSeriesRangeAsync(
                string key,
                long fromTimestamp,
                long toTimestamp,
                bool reverse,
                int? count,
                string? aggregationType,
                long? bucketDurationMs,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<TimeSeriesDeleteResult> TimeSeriesDeleteAsync(
                string key,
                long fromTimestamp,
                long toTimestamp,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<TimeSeriesInfoResult> TimeSeriesInfoAsync(
                string key,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    public Task<TimeSeriesMRangeResult> TimeSeriesMultiRangeAsync(
                long fromTimestamp,
                long toTimestamp,
                bool reverse,
                int? count,
                string? aggregationType,
                long? bucketDurationMs,
                KeyValuePair<string, string>[] filters,
                CancellationToken token = default)
    {
        throw new NotSupportedException("Operation is not implemented by SqlServerKeyValueStore yet.");
    }

    private async Task<byte[]?> GetCoreAsync(string key, CancellationToken token)
    {
        await EnsureSchemaAsync(token).ConfigureAwait(false);

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(token).ConfigureAwait(false);

        await using var command = connection.CreateCommand();
        command.CommandText = $"SELECT [Value] FROM [dbo].[{_tableName}] WHERE [Key] = @key AND ([ExpiresAt] IS NULL OR [ExpiresAt] > SYSUTCDATETIME())";
        command.Parameters.Add(new SqlParameter("@key", SqlDbType.NVarChar, 512) { Value = key });

        var value = await command.ExecuteScalarAsync(token).ConfigureAwait(false);
        return value is DBNull or null ? null : (byte[])value;
    }

    private async Task<bool> SetCoreAsync(string key, byte[] value, TimeSpan? expiration, SetCondition condition, CancellationToken token)
    {
        await EnsureSchemaAsync(token).ConfigureAwait(false);

        var expiresAt = expiration.HasValue ? DateTime.UtcNow.Add(expiration.Value) : (DateTime?)null;

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(token).ConfigureAwait(false);

        var exists = await ExistsCoreInternalAsync(connection, key, token).ConfigureAwait(false);
        if (condition == SetCondition.Nx && exists)
        {
            return false;
        }

        if (condition == SetCondition.Xx && !exists)
        {
            return false;
        }

        await using var command = connection.CreateCommand();
        if (exists)
        {
            command.CommandText = $"UPDATE [dbo].[{_tableName}] SET [Value] = @value, [ExpiresAt] = @expiresAt WHERE [Key] = @key";
        }
        else
        {
            command.CommandText = $"INSERT INTO [dbo].[{_tableName}] ([Key], [Value], [ExpiresAt]) VALUES (@key, @value, @expiresAt)";
        }

        command.Parameters.Add(new SqlParameter("@key", SqlDbType.NVarChar, 512) { Value = key });
        command.Parameters.Add(new SqlParameter("@value", SqlDbType.VarBinary, -1) { Value = value });
        command.Parameters.Add(new SqlParameter("@expiresAt", SqlDbType.DateTime2) { Value = expiresAt.HasValue ? expiresAt.Value : DBNull.Value });
        await command.ExecuteNonQueryAsync(token).ConfigureAwait(false);
        return true;
    }

    private async Task<byte[]?[]> GetManyCoreAsync(string[] keys, CancellationToken token)
    {
        var values = new byte[]?[keys.Length];
        for (var index = 0; index < keys.Length; index++)
        {
            values[index] = await GetCoreAsync(keys[index], token).ConfigureAwait(false);
        }

        return values;
    }

    private async Task<bool> SetManyCoreAsync(KeyValuePair<string, byte[]>[] items, CancellationToken token)
    {
        await EnsureSchemaAsync(token).ConfigureAwait(false);

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(token).ConfigureAwait(false);
        await using var transaction = (SqlTransaction)await connection.BeginTransactionAsync(token).ConfigureAwait(false);

        foreach (var item in items)
        {
            var exists = await ExistsCoreInternalAsync(connection, item.Key, token, transaction).ConfigureAwait(false);
            await using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = exists
                ? $"UPDATE [dbo].[{_tableName}] SET [Value] = @value, [ExpiresAt] = NULL WHERE [Key] = @key"
                : $"INSERT INTO [dbo].[{_tableName}] ([Key], [Value], [ExpiresAt]) VALUES (@key, @value, NULL)";

            command.Parameters.Add(new SqlParameter("@key", SqlDbType.NVarChar, 512) { Value = item.Key });
            command.Parameters.Add(new SqlParameter("@value", SqlDbType.VarBinary, -1) { Value = item.Value });
            await command.ExecuteNonQueryAsync(token).ConfigureAwait(false);
        }

        await transaction.CommitAsync(token).ConfigureAwait(false);
        return true;
    }

    private async Task<long> DeleteCoreAsync(string[] keys, CancellationToken token)
    {
        if (keys.Length == 0)
        {
            return 0;
        }

        await EnsureSchemaAsync(token).ConfigureAwait(false);

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(token).ConfigureAwait(false);

        var parameterNames = new string[keys.Length];
        await using var command = connection.CreateCommand();
        for (var index = 0; index < keys.Length; index++)
        {
            var parameterName = $"@p{index}";
            parameterNames[index] = parameterName;
            command.Parameters.Add(new SqlParameter(parameterName, SqlDbType.NVarChar, 512) { Value = keys[index] });
        }

        command.CommandText = $"DELETE FROM [dbo].[{_tableName}] WHERE [Key] IN ({string.Join(",", parameterNames)})";
        return await command.ExecuteNonQueryAsync(token).ConfigureAwait(false);
    }

    private async Task<bool> ExistsCoreAsync(string key, CancellationToken token)
    {
        await EnsureSchemaAsync(token).ConfigureAwait(false);

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(token).ConfigureAwait(false);
        return await ExistsCoreInternalAsync(connection, key, token).ConfigureAwait(false);
    }

    private async Task<long> ExistsManyCoreAsync(string[] keys, CancellationToken token)
    {
        if (keys.Length == 0)
        {
            return 0;
        }

        await EnsureSchemaAsync(token).ConfigureAwait(false);

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(token).ConfigureAwait(false);

        var parameterNames = new string[keys.Length];
        await using var command = connection.CreateCommand();
        for (var index = 0; index < keys.Length; index++)
        {
            var parameterName = $"@p{index}";
            parameterNames[index] = parameterName;
            command.Parameters.Add(new SqlParameter(parameterName, SqlDbType.NVarChar, 512) { Value = keys[index] });
        }

        command.CommandText = $"SELECT COUNT(1) FROM [dbo].[{_tableName}] WHERE [Key] IN ({string.Join(",", parameterNames)}) AND ([ExpiresAt] IS NULL OR [ExpiresAt] > SYSUTCDATETIME())";
        var result = await command.ExecuteScalarAsync(token).ConfigureAwait(false);
        return result is null or DBNull ? 0 : Convert.ToInt64(result, CultureInfo.InvariantCulture);
    }

    private async Task<long?> IncrByCoreAsync(string key, long delta, CancellationToken token)
    {
        await EnsureSchemaAsync(token).ConfigureAwait(false);

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(token).ConfigureAwait(false);
        await using var transaction = (SqlTransaction)await connection.BeginTransactionAsync(IsolationLevel.Serializable, token).ConfigureAwait(false);

        await using var selectCommand = connection.CreateCommand();
        selectCommand.Transaction = transaction;
        selectCommand.CommandText = $"SELECT [Value], [ExpiresAt] FROM [dbo].[{_tableName}] WITH (UPDLOCK, HOLDLOCK) WHERE [Key] = @key";
        selectCommand.Parameters.Add(new SqlParameter("@key", SqlDbType.NVarChar, 512) { Value = key });

        await using var reader = await selectCommand.ExecuteReaderAsync(token).ConfigureAwait(false);
        byte[]? currentBytes = null;
        DateTime? expiresAt = null;
        var hasRow = await reader.ReadAsync(token).ConfigureAwait(false);
        if (hasRow)
        {
            currentBytes = reader.IsDBNull(0) ? null : (byte[])reader.GetValue(0);
            expiresAt = reader.IsDBNull(1) ? null : reader.GetDateTime(1);
        }

        await reader.CloseAsync().ConfigureAwait(false);

        if (expiresAt.HasValue && expiresAt.Value <= DateTime.UtcNow)
        {
            await using var deleteExpiredCommand = connection.CreateCommand();
            deleteExpiredCommand.Transaction = transaction;
            deleteExpiredCommand.CommandText = $"DELETE FROM [dbo].[{_tableName}] WHERE [Key] = @key";
            deleteExpiredCommand.Parameters.Add(new SqlParameter("@key", SqlDbType.NVarChar, 512) { Value = key });
            await deleteExpiredCommand.ExecuteNonQueryAsync(token).ConfigureAwait(false);
            hasRow = false;
            currentBytes = null;
        }

        long currentValue = 0;
        if (hasRow)
        {
            var text = currentBytes is null ? "0" : Encoding.UTF8.GetString(currentBytes);
            if (!long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out currentValue))
            {
                await transaction.RollbackAsync(token).ConfigureAwait(false);
                return null;
            }
        }

        long nextValue;
        try
        {
            nextValue = checked(currentValue + delta);
        }
        catch (OverflowException)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return null;
        }

        var nextBytes = Encoding.UTF8.GetBytes(nextValue.ToString(CultureInfo.InvariantCulture));
        await using var writeCommand = connection.CreateCommand();
        writeCommand.Transaction = transaction;
        writeCommand.CommandText = hasRow
            ? $"UPDATE [dbo].[{_tableName}] SET [Value] = @value, [ExpiresAt] = NULL WHERE [Key] = @key"
            : $"INSERT INTO [dbo].[{_tableName}] ([Key], [Value], [ExpiresAt]) VALUES (@key, @value, NULL)";
        writeCommand.Parameters.Add(new SqlParameter("@key", SqlDbType.NVarChar, 512) { Value = key });
        writeCommand.Parameters.Add(new SqlParameter("@value", SqlDbType.VarBinary, -1) { Value = nextBytes });
        await writeCommand.ExecuteNonQueryAsync(token).ConfigureAwait(false);

        await transaction.CommitAsync(token).ConfigureAwait(false);
        return nextValue;
    }

    private async Task<bool> ExpireCoreAsync(string key, TimeSpan expiration, CancellationToken token)
    {
        if (expiration <= TimeSpan.Zero)
        {
            return false;
        }

        await EnsureSchemaAsync(token).ConfigureAwait(false);

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(token).ConfigureAwait(false);

        var expiresAt = DateTime.UtcNow.Add(expiration);
        await using var command = connection.CreateCommand();
        command.CommandText = $"UPDATE [dbo].[{_tableName}] SET [ExpiresAt] = @expiresAt WHERE [Key] = @key AND ([ExpiresAt] IS NULL OR [ExpiresAt] > SYSUTCDATETIME())";
        command.Parameters.Add(new SqlParameter("@key", SqlDbType.NVarChar, 512) { Value = key });
        command.Parameters.Add(new SqlParameter("@expiresAt", SqlDbType.DateTime2) { Value = expiresAt });
        var updated = await command.ExecuteNonQueryAsync(token).ConfigureAwait(false);
        return updated > 0;
    }

    private async Task<long> TtlCoreAsync(string key, bool milliseconds, CancellationToken token)
    {
        await EnsureSchemaAsync(token).ConfigureAwait(false);

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(token).ConfigureAwait(false);

        await using var command = connection.CreateCommand();
        command.CommandText = $"SELECT [ExpiresAt] FROM [dbo].[{_tableName}] WHERE [Key] = @key";
        command.Parameters.Add(new SqlParameter("@key", SqlDbType.NVarChar, 512) { Value = key });
        var value = await command.ExecuteScalarAsync(token).ConfigureAwait(false);

        if (value is null or DBNull)
        {
            await using var existsCommand = connection.CreateCommand();
            existsCommand.CommandText = $"SELECT COUNT(1) FROM [dbo].[{_tableName}] WHERE [Key] = @key";
            existsCommand.Parameters.Add(new SqlParameter("@key", SqlDbType.NVarChar, 512) { Value = key });
            var exists = Convert.ToInt64(await existsCommand.ExecuteScalarAsync(token).ConfigureAwait(false), CultureInfo.InvariantCulture);
            return exists > 0 ? -1 : -2;
        }

        var expiresAt = (DateTime)value;
        var remaining = expiresAt - DateTime.UtcNow;
        if (remaining <= TimeSpan.Zero)
        {
            await using var cleanupCommand = connection.CreateCommand();
            cleanupCommand.CommandText = $"DELETE FROM [dbo].[{_tableName}] WHERE [Key] = @key";
            cleanupCommand.Parameters.Add(new SqlParameter("@key", SqlDbType.NVarChar, 512) { Value = key });
            await cleanupCommand.ExecuteNonQueryAsync(token).ConfigureAwait(false);
            return -2;
        }

        return milliseconds ? (long)remaining.TotalMilliseconds : (long)remaining.TotalSeconds;
    }

    private async Task<long> CleanUpExpiredKeysCoreAsync(CancellationToken token)
    {
        await EnsureSchemaAsync(token).ConfigureAwait(false);

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(token).ConfigureAwait(false);

        await using var command = connection.CreateCommand();
        command.CommandText = $"DELETE FROM [dbo].[{_tableName}] WHERE [ExpiresAt] IS NOT NULL AND [ExpiresAt] <= SYSUTCDATETIME()";
        return await command.ExecuteNonQueryAsync(token).ConfigureAwait(false);
    }

    private async Task<bool> ExistsCoreInternalAsync(SqlConnection connection, string key, CancellationToken token, SqlTransaction? transaction = null)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = $"SELECT COUNT(1) FROM [dbo].[{_tableName}] WHERE [Key] = @key AND ([ExpiresAt] IS NULL OR [ExpiresAt] > SYSUTCDATETIME())";
        command.Parameters.Add(new SqlParameter("@key", SqlDbType.NVarChar, 512) { Value = key });
        var result = await command.ExecuteScalarAsync(token).ConfigureAwait(false);
        return result is not null && result is not DBNull && Convert.ToInt64(result, CultureInfo.InvariantCulture) > 0;
    }

    private async Task EnsureSchemaAsync(CancellationToken token)
    {
        if (_schemaInitialized)
        {
            return;
        }

        await _schemaLock.WaitAsync(token).ConfigureAwait(false);
        try
        {
            if (_schemaInitialized)
            {
                return;
            }

            await using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync(token).ConfigureAwait(false);

            await using var command = connection.CreateCommand();
            command.CommandText = $@"
IF OBJECT_ID(N'[dbo].[{_tableName}]', N'U') IS NULL
BEGIN
    CREATE TABLE [dbo].[{_tableName}] (
        [Key] NVARCHAR(512) NOT NULL CONSTRAINT [PK_{_tableName}] PRIMARY KEY,
        [Value] VARBINARY(MAX) NOT NULL,
        [ExpiresAt] DATETIME2 NULL
    );

    CREATE INDEX [IX_{_tableName}_ExpiresAt] ON [dbo].[{_tableName}] ([ExpiresAt]);
END";

            await command.ExecuteNonQueryAsync(token).ConfigureAwait(false);
            _schemaInitialized = true;
        }
        finally
        {
            _schemaLock.Release();
        }
    }

}


