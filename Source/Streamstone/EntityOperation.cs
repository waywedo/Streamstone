using System;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization;
using Azure.Data.Tables;

namespace Streamstone
{
    abstract class EntityOperation
    {
        public static readonly EntityOperation None = new Null();

        public readonly ITableEntity Entity;

        EntityOperation(ITableEntity entity)
        {
            Entity = entity;
        }

        protected abstract TableTransactionAction AsTableTransactionAction();

        public static implicit operator TableTransactionAction(EntityOperation arg)
        {
            return arg.AsTableTransactionAction();
        }

        public abstract EntityOperation Merge(EntityOperation other);

        Exception InvalidMerge(EntityOperation other)
        {
            var message = string.Format("Included '{0}' operation cannot be followed by '{1}' operation",
                GetType().Name, other.GetType().Name);

            return new InvalidOperationException(message);
        }

        public EntityOperation Apply(Partition partition)
        {
            Entity.PartitionKey = partition.PartitionKey;
            return this;
        }

        protected TableEntity TableEntity()
        {
            var entity = new TableEntity(Entity.PartitionKey, Entity.RowKey)
            {
                ETag = Entity.ETag
            };

            foreach (var property in Entity.GetType().GetTypeInfo().DeclaredProperties
                .Where(p => !IsReserved(p.Name)
                    && (p.PropertyType.IsAssignableTo(typeof(PropertyMap))
                        || p.GetCustomAttribute<IgnoreDataMemberAttribute>() == null)))
            {
                if (property.PropertyType.IsAssignableTo(typeof(PropertyMap)))
                {
                    foreach (var mappedProperty in (PropertyMap)property.GetValue(Entity))
                    {
                        entity.Add(mappedProperty.Key, mappedProperty.Value);
                    }
                    continue;
                }

                entity.Add(property.Name, property.GetValue(Entity));
            }

            return entity;
        }

        static bool IsReserved(string propertyName)
        {
            switch (propertyName)
            {
                case "PartitionKey":
                case "RowKey":
                case "ETag":
                case "Timestamp":
                    return true;
                default:
                    return false;
            }
        }

        public class Insert : EntityOperation
        {
            public Insert(ITableEntity entity)
                : base(entity)
            { }

            protected override TableTransactionAction AsTableTransactionAction()
            {
                return new TableTransactionAction(TableTransactionActionType.Add, TableEntity());
            }

            public override EntityOperation Merge(EntityOperation other)
            {
                if (other is Insert)
                    throw InvalidMerge(other);

                if (other is Replace)
                    return new Insert(other.Entity);

                if (other is Delete)
                    return None;

                if (other is InsertOrMerge)
                    throw InvalidMerge(other);

                if (other is InsertOrReplace)
                    throw InvalidMerge(other);

                throw new InvalidOperationException("Unsupported operation type: " + other.GetType());
            }
        }

        public class Replace : EntityOperation
        {
            public Replace(ITableEntity entity)
                : base(entity)
            { }

            protected override TableTransactionAction AsTableTransactionAction()
            {
                return new TableTransactionAction(TableTransactionActionType.UpdateReplace, TableEntity());
            }

            public override EntityOperation Merge(EntityOperation other)
            {
                if (other is Insert)
                    throw InvalidMerge(other);

                if (other is Replace)
                    return other;

                if (other is Delete)
                    return other;

                if (other is InsertOrMerge)
                    throw InvalidMerge(other);

                if (other is InsertOrReplace)
                    throw InvalidMerge(other);

                throw new InvalidOperationException("Unsupported operation type: " + other.GetType());
            }
        }

        public class Delete : EntityOperation
        {
            public Delete(ITableEntity entity)
                : base(entity)
            { }

            protected override TableTransactionAction AsTableTransactionAction()
            {
                return new TableTransactionAction(TableTransactionActionType.Delete, Entity);
            }

            public override EntityOperation Merge(EntityOperation other)
            {
                if (other is Insert)
                    return new Replace(other.Entity);

                if (other is Replace)
                    throw InvalidMerge(other);

                if (other is Delete)
                    throw InvalidMerge(other);

                if (other is InsertOrMerge)
                    throw InvalidMerge(other);

                if (other is InsertOrReplace)
                    throw InvalidMerge(other);

                throw new InvalidOperationException("Unsupported operation type: " + other.GetType());
            }
        }

        public class InsertOrMerge : EntityOperation
        {
            public InsertOrMerge(ITableEntity entity)
                : base(entity)
            { }

            protected override TableTransactionAction AsTableTransactionAction()
            {
                return new TableTransactionAction(TableTransactionActionType.UpsertMerge, TableEntity());
            }

            public override EntityOperation Merge(EntityOperation other)
            {
                if (other is Insert)
                    throw InvalidMerge(other);

                if (other is Replace)
                    throw InvalidMerge(other);

                if (other is Delete)
                    throw InvalidMerge(other);

                if (other is InsertOrMerge)
                    return other;

                if (other is InsertOrReplace)
                    throw InvalidMerge(other);

                throw new InvalidOperationException("Unsupported operation type: " + other.GetType());
            }
        }

        public class InsertOrReplace : EntityOperation
        {
            public InsertOrReplace(ITableEntity entity)
                : base(entity)
            { }

            protected override TableTransactionAction AsTableTransactionAction()
            {
                return new TableTransactionAction(TableTransactionActionType.UpsertReplace, TableEntity());
            }

            public override EntityOperation Merge(EntityOperation other)
            {
                if (other is Insert)
                    throw InvalidMerge(other);

                if (other is Replace)
                    throw InvalidMerge(other);

                if (other is Delete)
                    throw InvalidMerge(other);

                if (other is InsertOrMerge)
                    throw InvalidMerge(other);

                if (other is InsertOrReplace)
                    return other;

                throw new InvalidOperationException("Unsupported operation type: " + other.GetType());
            }
        }

        internal class UpdateMerge : EntityOperation
        {
            public UpdateMerge(ITableEntity entity)
                : base(entity)
            { }

            protected override TableTransactionAction AsTableTransactionAction()
            {
                return new TableTransactionAction(TableTransactionActionType.UpdateMerge, TableEntity());
            }

            public override EntityOperation Merge(EntityOperation other) =>
                throw new InvalidOperationException("Internal-only stream header merge operation");
        }

        class Null : EntityOperation
        {
            internal Null()
                : base(null)
            { }

            protected override TableTransactionAction AsTableTransactionAction()
            {
                throw new NotImplementedException();
            }

            public override EntityOperation Merge(EntityOperation other)
            {
                if (other is Insert)
                    return other;

                if (other is Replace)
                    throw InvalidMerge(other);

                if (other is Delete)
                    throw InvalidMerge(other);

                if (other is InsertOrMerge)
                    return other;

                if (other is InsertOrReplace)
                    return other;

                throw new InvalidOperationException("Unsupported operation type: " + other.GetType());
            }

            new static Exception InvalidMerge(EntityOperation other)
            {
                var message = string.Format("Included 'Delete' operation interdifused with " +
                                            "preceding 'Insert' operation. " +
                                            "'{0}' cannot be applied to NULL",
                    other.GetType());

                return new InvalidOperationException(message);
            }
        }
    }
}