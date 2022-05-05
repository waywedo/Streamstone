using Azure.Data.Tables;
using System.Collections.Generic;
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
            public static async Task<IEnumerable<TEntity>> RowKeyPrefixQueryAsync<TEntity>(this Partition partition, string prefix)
                where TEntity : class, ITableEntity, new()
            {
                var filter = $"{nameof(TableEntity.PartitionKey)} eq '{partition.PartitionKey}'" +
                    $" and {WhereRowKeyPrefixFilter(prefix)}";

                return await ExecuteQueryAsync<TEntity>(partition.Table, filter);
            }

            /// <summary>
            /// Applies row key prefix criteria to given table which allows to query a range of rows
            /// </summary>
            /// <typeparam name="TEntity">The type of the entity to return.</typeparam>
            /// <param name="table">The table.</param>
            /// <param name="filter">The row key prefix filter.</param>
            /// <returns>An instance of <see cref="IEnumerable{T}"/> that alllow further criterias to be added</returns>
            public static async Task<IEnumerable<TEntity>> ExecuteQueryAsync<TEntity>(this TableClient table, string filter)
                where TEntity : class, ITableEntity, new()
            {
                var query = table.QueryAsync<TEntity>(filter);

                var result = new List<TEntity>();
                await foreach (var entity in query)
                {
                    result.Add(entity);
                }

                return result;
            }

            static string WhereRowKeyPrefixFilter(string prefix)
            {
                var range = new PrefixRange(prefix);

                return $"{nameof(TableEntity.RowKey)} ge '{range.Start}'" +
                    $" and {nameof(TableEntity.RowKey)} lt '{range.End}'";
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