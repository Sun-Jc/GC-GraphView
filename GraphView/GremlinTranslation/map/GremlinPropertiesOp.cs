﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    internal class GremlinPropertiesOp: GremlinTranslationOperator
    {
        public List<string> PropertyKeys;

        public GremlinPropertiesOp(params string[] propertyKeys)
        {
            PropertyKeys = new List<string>(propertyKeys);
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            inputContext.PivotVariable.Properties(inputContext, PropertyKeys);

            return inputContext;
        }
    }
}
