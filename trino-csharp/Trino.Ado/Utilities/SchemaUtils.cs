using Trino.Core.Logging;
using Trino.Core.Utils;
using Trino.Core;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Trino.Ado.Server;

namespace Trino.Ado.Utilities
{
    /// <summary>
    /// Provides functions to fetch schema information from Trino. Aligned to the SQL server implementation.
    /// </summary>
    internal class SchemaUtils
    {
        private static readonly Regex _legalIdentifierName = new("^[a-zA-Z_][a-zA-Z_0-9]*$");

        internal static DataTable GetInformationSchema(TrinoConnection connection, ILoggerWrapper? logger, string informationSchemaTable, string filter)
        {
            if (string.IsNullOrEmpty(connection.ConnectionSession.Properties.Catalog))
            {
                // If no catalog is specified, we query all catalogs and this cannot be accomplished in one query.
                return GetAllInformationSchemaWithTimeout(connection, logger, informationSchemaTable, filter);
            }
            else
            {
                var whereIfFilter = string.IsNullOrEmpty(filter) ? "" : "WHERE";
                return new TrinoCommand(connection, $"SELECT * FROM {connection.ConnectionSession.Properties.Catalog}.information_schema.{informationSchemaTable} {whereIfFilter} {filter}").RunQuery().SafeResult().BuildDataTableAsync().SafeResult();
            }
        }

        internal static string BuildFilterForRestrictions(ClientSession session, TrinoSchemaRestriction[] restrictionMapping, string?[]? restrictionValues)
        {
            ValidateRestrictionValues(restrictionMapping, restrictionValues);

            var restrictionsFilters = new List<string>();
            for (var i = 0; i < restrictionMapping.Length; i++)
            {
                var colName = restrictionMapping[i].TrinoColumnName;
                if (restrictionValues != null && i < restrictionValues.Length && restrictionValues[i] != null)
                {
                    restrictionsFilters.Add($"{colName} = '{restrictionValues[i]}'");
                }
                // If the restriction value is null, we use the session properties to filter the results.
                else if (!string.IsNullOrEmpty(session.Properties.Catalog) && restrictionMapping[i].RestrictionType == SchemaRestrictionType.Catalog)
                {
                    restrictionsFilters.Add($"{colName} = '{session.Properties.Catalog}'");
                }
                else if (!string.IsNullOrEmpty(session.Properties.Schema) && restrictionMapping[i].RestrictionType == SchemaRestrictionType.Schema)
                {
                    restrictionsFilters.Add($"{colName} = '{session.Properties.Schema}'");
                }
            }
            return string.Join(" AND ", restrictionsFilters);
        }

        internal static TrinoSchemaRestriction[] SchemaRestrictionsMapping =
        [
            new("schema_name", SchemaRestrictionType.Schema)
        ];

        internal static TrinoSchemaRestriction[] TableRestrictionsMapping =
        [
            new("table_catalog", SchemaRestrictionType.Catalog),
            new("table_schema", SchemaRestrictionType.Schema),
            new("table_name", SchemaRestrictionType.Table),
            new("table_type", SchemaRestrictionType.TableType)
        ];

        internal static TrinoSchemaRestriction[] ColumnRestrictionsMapping =
        [
            new("table_catalog", SchemaRestrictionType.Catalog),
            new("table_schema", SchemaRestrictionType.Schema),
            new("table_name", SchemaRestrictionType.Table),
            new("column_name", SchemaRestrictionType.Column)
        ];

        internal static TrinoSchemaRestriction[] ViewRestrictionsMapping =
        [
            new("table_catalog", SchemaRestrictionType.Catalog),
            new("table_schema", SchemaRestrictionType.Schema),
            new("table_name", SchemaRestrictionType.Table)
        ];

        private static void ValidateRestrictionValues(TrinoSchemaRestriction[] restrictionMapping, string?[]? restrictionValues)
        {
            if (restrictionValues == null) return;
            if (restrictionValues.Length > restrictionMapping.Length)
            {
                throw new ArgumentException($"Expected {restrictionMapping.Length} restriction values, but got {restrictionValues.Length}. Legal restriction mappings are: {string.Join(", ", restrictionMapping.Select(cr => cr.TrinoColumnName))}");
            }

            // Prevent SQL injection by limiting restriction values to legal table names which means alphanumeric and underscores
            foreach (var value in restrictionValues)
            {
                if (!string.IsNullOrEmpty(value) && !_legalIdentifierName.IsMatch(value))
                {
                    throw new ArgumentException($"Illegal restriction value {value}. Restriction values must be alphanumeric and underscores.");
                }
            }
        }

        /// <summary>
        /// Queries catalogs separately, ignoring any that do not respond in the timeout configured in the session.
        /// </summary>
        private static DataTable GetAllInformationSchemaWithTimeout(TrinoConnection connection, ILoggerWrapper? logger, string informationSchemaTable, string filter)
        {
            var catalogs = new TrinoCommand(connection, "SHOW CATALOGS").RunQuery().SafeResult().Select(row => row[0].ToString()).ToList();
            var schemas = new ConcurrentBag<DataTable>();
            // union all query will fail if any catalog does not respond, so we issue a query per catalog respecting the timeout
            Parallel.ForEach(catalogs, catalog =>
            {
                try
                {
                    var command = $"SELECT * FROM {catalog}.information_schema.{informationSchemaTable} WHERE {filter}";
                    schemas.Add(new TrinoCommand(connection, command).RunQuery().SafeResult().BuildDataTableAsync().SafeResult());
                }
                catch (TrinoAggregateException e)
                {
                    // Some catalogs can be broken or slow to respond. This avoids blocking the entire query.
                    if (e.InnerExceptions.Any(ex => ex is TimeoutException))
                    {
                        logger?.LogWarning($"Catalog {catalog} did not respond to the query in time. Skipping.");
                    }
                    else
                    {
                        throw;
                    }
                }
            });

            DataTable? merged = null;
            foreach (var dt in schemas)
            {
                if (merged == null)
                {
                    merged = dt;
                }
                else
                {
                    dt.Merge(merged);
                }
            }
            return merged ?? new DataTable();
        }
    }
}
