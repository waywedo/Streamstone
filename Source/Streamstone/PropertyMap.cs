﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Azure.Data.Tables;

namespace Streamstone
{
    public abstract class PropertyMap : Dictionary<string, object>
    {
        static readonly object[] NoArgs = Array.Empty<object>();

        protected PropertyMap()
        {
        }

        protected PropertyMap(IDictionary<string, object> properties)
            : base(properties)
        {
        }

        protected static IDictionary<string, object> ToDictionary(object obj)
        {
            Requires.NotNull(obj, nameof(obj));

            return obj.GetType().GetTypeInfo().DeclaredProperties
                .ToDictionary(p => p.Name, p => p.GetValue(obj, NoArgs));
        }

        public abstract void CopyFrom(TableEntity entity);
    }
}
