using Azure;
using Azure.Data.Tables;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization;
using System.Threading;
using System.Threading.Tasks;

namespace Streamstone
{
    namespace Utility
    {
        /// <summary>
        /// The set of helper extension methods for querying WATS tables
        /// </summary>
        public static class TableQueryExtensions
        {
            /// <summary>
            /// Query range of rows in partition having the same row key prefix
            /// </summary>
            /// <typeparam name="TEntity">The type of the entity to return.</typeparam>
            /// <param name="partition">The partition.</param>
            /// <param name="prefix">The row key prefix.</param>
            /// <returns>An instance of <see cref="IEnumerable{T}"/> that allow to scroll over all rows</returns>
            public static async Task<IEnumerable<TEntity>> RowKeyPrefixQueryAsync<TEntity>(this Partition partition, string prefix, CancellationToken ct)
                where TEntity : class, ITableEntity, new()
            {
                var filter = $"{nameof(ITableEntity.PartitionKey)} eq '{partition.PartitionKey}'" +
                    $" and {WhereRowKeyPrefixFilter(prefix)}";

                return (await ExecuteQueryAsync<TableEntity>(partition.Table, filter, ct))
                    .Select(e => e.AsEntity<TEntity>());
            }

            /// <summary>
            /// Applies row key prefix criteria to given table which allows to query a range of rows
            /// </summary>
            /// <typeparam name="TEntity">The type of the entity to return.</typeparam>
            /// <param name="table">The table.</param>
            /// <param name="filter">The row key prefix filter.</param>
            /// <returns>An instance of <see cref="IEnumerable{T}"/> that alllow further criterias to be added</returns>
            public static async Task<IEnumerable<TEntity>> ExecuteQueryAsync<TEntity>(this TableClient table, string filter, CancellationToken ct)
                where TEntity : class, ITableEntity, new()
            {
                var query = table.QueryAsync<TableEntity>(filter, cancellationToken: ct);

                var result = new List<TEntity>();
                await foreach (var entity in query)
                {
                    result.Add(entity.AsEntity<TEntity>());
                }

                return result;
            }

            static string WhereRowKeyPrefixFilter(string prefix)
            {
                var range = new PrefixRange(prefix);

                return $"{nameof(ITableEntity.RowKey)} ge '{range.Start}'" +
                    $" and {nameof(ITableEntity.RowKey)} lt '{range.End}'";
            }
        }

        public static class TableEntityExtensions
        {
            public static T AsEntity<T>(this TableEntity entity)
                where T : class, new()
            {
                if (typeof(T) == typeof(TableEntity))
                    return entity as T;

                var t = new T();
                entity.CopyTo(t);

                return t;
            }

            public static void CopyTo(this TableEntity entity, object target)
            {
                foreach (var property in target.GetType().GetTypeInfo().DeclaredProperties
                    .Where(p => p.PropertyType.IsAssignableTo(typeof(PropertyMap))
                        || p.GetCustomAttribute<IgnoreDataMemberAttribute>() == null))
                {
                    if (property.PropertyType.IsAssignableTo(typeof(PropertyMap)))
                    {
                        var propertyMap = (PropertyMap)property.GetValue(target);
                        propertyMap.CopyFrom(entity);
                        property.SetValue(target, propertyMap);
                        continue;
                    }

                    if (property.Name == "ETag")
                    {
                        property.SetValue(target, entity.ETag);
                    }
                    else if (entity.TryGetValue(property.Name, out var value))
                    {
                        property.SetValue(target, value);
                    }
                    else
                    {
                        throw new Exception($"Failed to find value for property '{property.Name}' when copying TableEntity to {target.GetType().Name}");
                    }
                }
            }
        }

        /// <summary>
        /// Represents lexicographical range
        /// </summary>
        public struct PrefixRange
        {
            /// <summary>
            /// The start of the range
            /// </summary>
            public readonly string Start;

            /// <summary>
            /// The end of the range
            /// </summary>
            public readonly string End;

            /// <summary>
            /// Initializes a new instance of the <see cref="PrefixRange"/> struct.
            /// </summary>
            /// <param name="prefix">The prefix upon which to build a range.</param>
            public PrefixRange(string prefix)
            {
                Requires.NotNullOrEmpty(prefix, nameof(prefix));

                Start = prefix;

                var length = prefix.Length - 1;
                var lastChar = prefix[length];
                var nextLastChar = (char)(lastChar + 1);

                End = prefix.Substring(0, length) + nextLastChar;
            }
        }
    }
}