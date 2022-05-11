using Azure;
using Azure.Data.Tables;
using Azure.Data.Tables.Models;
using Streamstone.Utility;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;

namespace Streamstone
{
    public sealed partial class Stream
    {
        class ProvisionOperation
        {
            readonly Stream stream;
            readonly TableClient table;

            public ProvisionOperation(Stream stream)
            {
                Debug.Assert(stream.IsTransient);

                this.stream = stream;
                table = stream.Partition.Table;
            }

            public async Task<Stream> ExecuteAsync(CancellationToken ct)
            {
                var insert = new Insert(stream);

                try
                {
                    return insert.Result(await insert.ExecuteAsync(table, ct).ConfigureAwait(false));
                }
                catch (RequestFailedException e)
                {
                    insert.Handle(e);
                    return null;
                }
            }

            class Insert
            {
                readonly TableEntity entity;
                readonly Partition partition;

                public Insert(Stream stream)
                {
                    entity = stream.TableEntity();
                    partition = stream.Partition;
                }

                internal async Task<Response> ExecuteAsync(TableClient table, CancellationToken ct)
                {
                    return await table.AddEntityAsync(entity, ct);
                }

                internal void Handle(RequestFailedException exception)
                {
                    if (exception.ErrorCode == TableErrorCode.EntityAlreadyExists)
                        throw ConcurrencyConflictException.StreamChangedOrExists(partition);

                    ExceptionDispatchInfo.Capture(exception).Throw();
                }

                internal Stream Result(Response response)
                {
                    entity.ETag = response.Headers.ETag.Value;
                    return From(partition, entity);
                }
            }
        }

        class WriteOperation
        {
            const int MaxOperationsPerChunk = 99;

            readonly Stream stream;
            readonly StreamWriteOptions options;
            readonly TableClient table;
            readonly IEnumerable<RecordedEvent> events;

            public WriteOperation(Stream stream, StreamWriteOptions options, IEnumerable<EventData> events)
            {
                this.stream = stream;
                this.options = options;
                this.events = stream.Record(events);
                table = stream.Partition.Table;
            }

            public async Task<StreamWriteResult> ExecuteAsync(CancellationToken ct)
            {
                var current = stream;

                foreach (var chunk in Chunks())
                {
                    var batch = chunk.ToBatch(current, options);

                    try
                    {
                        current = batch.Result(await table.SubmitTransactionAsync(batch.Prepare(), ct).ConfigureAwait(false));
                    }
                    catch (TableTransactionFailedException e)
                    {
                        batch.Handle(e);
                    }
                }

                return new StreamWriteResult(current, events.ToArray());
            }

            IEnumerable<Chunk> Chunks() => Chunk.Split(events).Where(s => !s.IsEmpty);

            class Chunk
            {
                public static IEnumerable<Chunk> Split(IEnumerable<RecordedEvent> events)
                {
                    var current = new Chunk();

                    foreach (var @event in events)
                    {
                        var next = current.Add(@event);

                        if (next != current)
                            yield return current;

                        current = next;
                    }

                    yield return current;
                }

                readonly List<RecordedEvent> events = new List<RecordedEvent>();
                int operations;

                Chunk()
                { }

                Chunk(RecordedEvent first) => Accomodate(first);

                Chunk Add(RecordedEvent @event)
                {
                    if (@event.Operations > MaxOperationsPerChunk)
                        throw new InvalidOperationException(
                            string.Format("{0} include(s) in event {1}:{{{2}}}, plus event entity itself, is over Azure's max batch size limit [{3}]",
                                          @event.IncludedOperations.Length, @event.Version, @event.Id, MaxOperationsPerChunk));

                    if (!CanAccomodate(@event))
                        return new Chunk(@event);

                    Accomodate(@event);
                    return this;
                }

                void Accomodate(RecordedEvent @event)
                {
                    operations += @event.Operations;
                    events.Add(@event);
                }

                bool CanAccomodate(RecordedEvent @event)
                {
                    return operations + @event.Operations <= MaxOperationsPerChunk;
                }

                public bool IsEmpty => events.Count == 0;

                public Batch ToBatch(Stream stream, StreamWriteOptions options)
                {
                    var entity = stream.Entity();
                    entity.Version += events.Count;
                    return new Batch(entity, events, options);
                }
            }

            class Batch
            {
                readonly List<EntityOperation> operations =
                     new List<EntityOperation>();

                readonly StreamEntity stream;
                readonly List<RecordedEvent> events;
                readonly StreamWriteOptions options;
                readonly Partition partition;

                internal Batch(StreamEntity stream, List<RecordedEvent> events, StreamWriteOptions options)
                {
                    this.stream = stream;
                    this.events = events;
                    this.options = options;
                    partition = stream.Partition;
                }

                internal List<TableTransactionAction> Prepare()
                {
                    WriteStream();
                    WriteEvents();
                    WriteIncludes();

                    return ToBatch();
                }

                void WriteStream() => operations.Add(stream.Operation());

                void WriteEvents() => operations.AddRange(events.SelectMany(e => e.EventOperations));

                void WriteIncludes()
                {
                    if (!options.TrackChanges)
                    {
                        operations.AddRange(events.SelectMany(x => x.IncludedOperations));
                        return;
                    }

                    var tracker = new EntityChangeTracker();

                    foreach (var @event in events)
                        tracker.Record(@event.IncludedOperations);

                    operations.AddRange(tracker.Compute());
                }

                List<TableTransactionAction> ToBatch()
                {
                    var result = new List<TableTransactionAction>();

                    foreach (var each in operations)
                        result.Add(each);

                    return result;
                }

                internal Stream Result(Response<IReadOnlyList<Response>> response)
                {
                    var streamIndex = operations.FindIndex(o => o.Entity.RowKey == stream.RowKey);
                    var streamResponse = response.Value[streamIndex];
                    stream.ETag = streamResponse.Headers.ETag.Value;

                    return From(partition, stream);
                }

                internal void Handle(TableTransactionFailedException exception)
                {
                    if (exception.ErrorCode == TableErrorCode.UpdateConditionNotSatisfied)
                        throw ConcurrencyConflictException.StreamChangedOrExists(partition);

                    if (exception.ErrorCode != TableErrorCode.EntityAlreadyExists)
                        ExceptionDispatchInfo.Capture(exception).Throw();

                    var conflicting = operations[exception.FailedTransactionActionIndex.Value].Entity;

                    if (conflicting == stream)
                        throw ConcurrencyConflictException.StreamChangedOrExists(partition);

                    if (conflicting is EventIdEntity id)
                        throw new DuplicateEventException(partition, id.Event.Id);

                    if (conflicting is EventEntity @event)
                        throw ConcurrencyConflictException.EventVersionExists(partition, @event.Version);

                    var include = operations.Single(x => x.Entity == conflicting);
                    throw IncludedOperationConflictException.Create(partition, include);
                }
            }
        }

        class SetPropertiesOperation
        {
            readonly Stream stream;
            readonly TableClient table;
            readonly StreamProperties properties;

            public SetPropertiesOperation(Stream stream, StreamProperties properties)
            {
                this.stream = stream;
                this.properties = properties;
                table = stream.Partition.Table;
            }

            public async Task<Stream> ExecuteAsync(CancellationToken ct)
            {
                var replace = new Replace(stream, properties);

                try
                {
                    return replace.Result(await replace.ExecuteAsync(table, ct).ConfigureAwait(false));
                }
                catch (RequestFailedException e)
                {
                    replace.Handle(e);
                    return null;
                }
            }

            class Replace
            {
                readonly TableEntity entity;
                readonly Partition partition;

                public Replace(Stream stream, StreamProperties properties)
                {
                    entity = stream.TableEntity(properties);
                    partition = stream.Partition;
                }

                internal async Task<Response> ExecuteAsync(TableClient table, CancellationToken ct)
                {
                    return await table.UpdateEntityAsync(entity, entity.ETag, TableUpdateMode.Replace, ct);
                }

                internal void Handle(RequestFailedException exception)
                {
                    if (exception.ErrorCode == TableErrorCode.UpdateConditionNotSatisfied)
                        throw ConcurrencyConflictException.StreamChanged(partition);

                    ExceptionDispatchInfo.Capture(exception).Throw();
                }

                internal Stream Result(Response response)
                {
                    entity.ETag = response.Headers.ETag.Value;
                    return From(partition, entity);
                }
            }
        }

        class OpenStreamOperation
        {
            readonly Partition partition;
            readonly TableClient table;

            public OpenStreamOperation(Partition partition)
            {
                this.partition = partition;
                table = partition.Table;
            }

            public async Task<StreamOpenResult> ExecuteAsync(CancellationToken ct)
            {
                try
                {
                    var entity = await table.GetEntityAsync<TableEntity>(partition.PartitionKey, partition.StreamRowKey(), null, ct);
                    return new StreamOpenResult(true, From(partition, entity));
                }
                catch (RequestFailedException)
                {
                    return StreamOpenResult.NotFound;
                }
            }
        }

        class ReadOperation<T> where T : class
        {
            readonly Partition partition;
            readonly TableClient table;

            readonly long startVersion;
            readonly long sliceSize;

            public ReadOperation(Partition partition, long startVersion, long sliceSize)
            {
                this.partition = partition;
                this.startVersion = startVersion;
                this.sliceSize = sliceSize;
                table = partition.Table;
            }

            public async Task<StreamSlice<T>> ExecuteAsync(Func<TableEntity, T> transform, CancellationToken ct)
            {
                var eventsQuery = ExecuteQueryAsync(EventsQuery(ct));
                var streamRowQuery = ExecuteQueryAsync(StreamRowQuery(ct));
                await Task.WhenAll(eventsQuery, streamRowQuery);

                return Result(await eventsQuery, FindStreamEntity(await streamRowQuery), transform);
            }

            StreamSlice<T> Result(List<TableEntity> entities, TableEntity streamEntity, Func<TableEntity, T> transform)
            {
                var stream = BuildStream(streamEntity);
                var events = BuildEvents(entities, transform);

                return new StreamSlice<T>(stream, events, startVersion, sliceSize);
            }

            AsyncPageable<TableEntity> EventsQuery(CancellationToken ct)
            {
                var rowKeyStart = partition.EventVersionRowKey(startVersion);
                var rowKeyEnd = partition.EventVersionRowKey(startVersion + sliceSize - 1);

                var filter = $"{nameof(ITableEntity.PartitionKey)} eq '{partition.PartitionKey}'" +
                    $" and {nameof(ITableEntity.RowKey)} ge '{rowKeyStart}'" +
                    $" and {nameof(ITableEntity.RowKey)} le '{rowKeyEnd}'";

                return table.QueryAsync<TableEntity>(filter, cancellationToken: ct);
            }

            AsyncPageable<TableEntity> StreamRowQuery(CancellationToken ct)
            {
                var filter = $"{nameof(ITableEntity.PartitionKey)} eq '{partition.PartitionKey}'" +
                    $" and {nameof(ITableEntity.RowKey)} eq '{partition.StreamRowKey()}'";

                return table.QueryAsync<TableEntity>(filter, cancellationToken: ct);
            }

            static async Task<List<TableEntity>> ExecuteQueryAsync(AsyncPageable<TableEntity> query)
            {
                var result = new List<TableEntity>();
                await foreach(var entity in query)
                {
                    result.Add(entity);
                }

                return result;
            }

            TableEntity FindStreamEntity(IEnumerable<TableEntity> entities)
            {
                var result = entities.SingleOrDefault(x => x.RowKey == partition.StreamRowKey());

                if (result == null)
                    throw new StreamNotFoundException(partition);

                return result;
            }

            Stream BuildStream(TableEntity entity) => From(partition, entity);

            static T[] BuildEvents(IEnumerable<TableEntity> entities, Func<TableEntity, T> transform) =>
                entities.Select(transform).ToArray();
        }
    }
}
