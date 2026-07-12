"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.MssqlDbParser = void 0;
const kysely_1 = require("kysely");
const db_parser_1 = require("./db.parser");
const UPDATED_AT_COLUMN_NAMES = ["updatedAt", "updated_at", "createdAt", "created_at"];
/**
 * SQL Server flavour of the schema parser. Reads INFORMATION_SCHEMA plus the
 * sys.* catalog views (FKs and indexes are not reliably exposed through
 * INFORMATION_SCHEMA on MSSQL) and maps the results into the same
 * DBManagerTypes shapes the Postgres parser produces. All shared logic
 * (getSchemaDetails, Prisma implicit M2M folding) is inherited from DbParser.
 */
class MssqlDbParser extends db_parser_1.DbParser {
    constructor(dbHandler, schema) {
        super(dbHandler);
        this.schema = schema || "dbo";
    }
    async getTables() {
        const [columns, keyColumns] = await Promise.all([
            (0, kysely_1.sql) `
                SELECT
                    c.TABLE_NAME     AS table_name,
                    c.COLUMN_NAME    AS name,
                    c.DATA_TYPE      AS type,
                    c.IS_NULLABLE    AS is_nullable,
                    c.COLUMN_DEFAULT AS column_default,
                    COLUMNPROPERTY(
                        OBJECT_ID(QUOTENAME(c.TABLE_SCHEMA) + '.' + QUOTENAME(c.TABLE_NAME)),
                        c.COLUMN_NAME,
                        'IsIdentity'
                    ) AS is_identity
                FROM INFORMATION_SCHEMA.COLUMNS c
                WHERE c.TABLE_SCHEMA = ${this.schema}
                ORDER BY c.TABLE_NAME, c.ORDINAL_POSITION
            `.execute(this.client),
            (0, kysely_1.sql) `
                SELECT
                    tc.CONSTRAINT_TYPE AS constraint_type,
                    kcu.TABLE_NAME     AS table_name,
                    kcu.COLUMN_NAME    AS column_name
                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                    ON kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
                   AND kcu.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA
                WHERE kcu.TABLE_SCHEMA = ${this.schema}
                  AND tc.CONSTRAINT_TYPE IN ('PRIMARY KEY', 'FOREIGN KEY', 'UNIQUE')
            `.execute(this.client),
        ]);
        const pkColumns = new Set();
        const fkColumns = new Set();
        const uniqueColumns = new Set();
        keyColumns.rows.forEach((r) => {
            const key = `${r.table_name}.${r.column_name}`;
            if (r.constraint_type === "PRIMARY KEY")
                pkColumns.add(key);
            else if (r.constraint_type === "FOREIGN KEY")
                fkColumns.add(key);
            else if (r.constraint_type === "UNIQUE")
                uniqueColumns.add(key);
        });
        const tables = [];
        columns.rows.forEach((row) => {
            const { table_name, name, type, is_nullable, column_default, is_identity } = row;
            let table = tables.find((t) => t.name === table_name);
            if (!table) {
                table = {
                    name: table_name,
                    columns: [],
                    constraints: [],
                    indexes: [],
                    customization: {
                        rename: "",
                        isVisible: true,
                        allowExport: true,
                        allowCreate: true,
                        allowEdit: true,
                        allowDelete: true,
                    }
                };
                tables.push(table);
            }
            const key = `${table_name}.${name}`;
            const primary_key = pkColumns.has(key);
            const foreign_key = fkColumns.has(key);
            const unique = uniqueColumns.has(key) || primary_key;
            const autoincrement = is_identity === 1 || is_identity === true;
            const defaultValue = this.normalizeDefault(column_default);
            const isTextual = /char|text/i.test(String(type ?? ""));
            const isUpdatedAt = UPDATED_AT_COLUMN_NAMES.includes(name) &&
                /date|timestamp/i.test(String(type ?? ""));
            let generatedType = "none";
            if (autoincrement) {
                generatedType = "sequence";
            }
            else if (/newid|newsequentialid|uuid/i.test(String(defaultValue ?? ""))) {
                generatedType = "uuid";
            }
            else if (primary_key && isTextual && !defaultValue) {
                // Prisma cuid ids are client-generated string PKs with no DB default
                generatedType = "cuid";
            }
            table.columns.push({
                name,
                type,
                nullable: is_nullable === "YES",
                default: defaultValue ?? (isUpdatedAt ? "CURRENT_TIMESTAMP" : null),
                primary_key,
                foreign_key,
                unique,
                autoincrement,
                isGenerated: generatedType !== "none",
                generatedType,
                enum_values: [], // SQL Server has no native enum type
                customization: {
                    description: "",
                    rename: "",
                    hideView: false,
                    hideEdit: false,
                    hideCreate: false,
                    readOnly: false,
                    position: 0,
                    displayType: "",
                    editType: "",
                    displayPrefix: "",
                    displaySuffix: "",
                }
            });
        });
        return tables;
    }
    async getConstraints() {
        const foreignKeys = await (0, kysely_1.sql) `
            SELECT
                tp.name AS table_name,
                fk.name AS name,
                cp.name AS column_name,
                tr.name AS reference_table,
                cr.name AS reference_column,
                REPLACE(fk.update_referential_action_desc, '_', ' ') AS update_rule,
                REPLACE(fk.delete_referential_action_desc, '_', ' ') AS delete_rule,
                CASE WHEN EXISTS (
                    SELECT 1
                    FROM sys.index_columns ic
                    JOIN sys.indexes i
                        ON i.object_id = ic.object_id
                       AND i.index_id = ic.index_id
                    WHERE ic.object_id = fkc.parent_object_id
                      AND ic.column_id = fkc.parent_column_id
                      AND i.is_unique = 1
                ) THEN 1 ELSE 0 END AS is_unique_fk
            FROM sys.foreign_keys fk
            JOIN sys.foreign_key_columns fkc
                ON fkc.constraint_object_id = fk.object_id
            JOIN sys.tables tp
                ON tp.object_id = fk.parent_object_id
            JOIN sys.columns cp
                ON cp.object_id = fkc.parent_object_id
               AND cp.column_id = fkc.parent_column_id
            JOIN sys.tables tr
                ON tr.object_id = fk.referenced_object_id
            JOIN sys.columns cr
                ON cr.object_id = fkc.referenced_object_id
               AND cr.column_id = fkc.referenced_column_id
            WHERE SCHEMA_NAME(tp.schema_id) = ${this.schema}
            ORDER BY tp.name, fk.name
        `.execute(this.client);
        const constraints = [];
        foreignKeys.rows.forEach((row) => {
            const { table_name, name, column_name, reference_table, reference_column, update_rule, delete_rule, is_unique_fk } = row;
            const isUnique = Boolean(is_unique_fk);
            const relationshipType = isUnique ? "one-to-one" : "one-to-many";
            let constraint = constraints.find((c) => c.table === table_name && c.name === name);
            if (!constraint) {
                constraint = {
                    table: table_name,
                    name,
                    column: column_name,
                    reference: reference_table
                        ? { table: reference_table, column: reference_column }
                        : null,
                    onUpdate: update_rule,
                    onDelete: delete_rule,
                    relationshipType,
                    isUnique,
                };
                constraints.push(constraint);
            }
        });
        return constraints;
    }
    async getIndexes() {
        const indexList = await (0, kysely_1.sql) `
            SELECT
                i.name AS index_name,
                t.name AS table_name,
                c.name AS column_name,
                i.is_primary_key AS is_primary,
                i.is_unique AS is_unique
            FROM sys.indexes i
            JOIN sys.tables t
                ON t.object_id = i.object_id
            JOIN sys.index_columns ic
                ON ic.object_id = i.object_id
               AND ic.index_id = i.index_id
            JOIN sys.columns c
                ON c.object_id = ic.object_id
               AND c.column_id = ic.column_id
            WHERE SCHEMA_NAME(t.schema_id) = ${this.schema}
              AND i.name IS NOT NULL
              AND i.is_hypothetical = 0
            ORDER BY t.name, i.name, ic.key_ordinal
        `.execute(this.client);
        const indexes = [];
        indexList.rows.forEach((row) => {
            const { table_name, index_name, column_name, is_primary, is_unique } = row;
            // MSSQL index names are only unique per table, so dedupe by
            // table + name rather than name alone.
            let index = indexes.find((i) => i.table === table_name && i.name === index_name);
            if (!index) {
                index = {
                    table: table_name,
                    name: index_name,
                    columns: [],
                    is_primary: Boolean(is_primary),
                    is_unique: Boolean(is_unique),
                };
                indexes.push(index);
            }
            index.columns.push(column_name);
        });
        return indexes;
    }
    /**
     * MSSQL wraps column defaults in parentheses, e.g. "((0))" or
     * "(getdate())". Strip the outer wrapping so consumers see the bare value.
     */
    normalizeDefault(def) {
        if (def == null)
            return null;
        let value = String(def).trim();
        while (value.startsWith("(") && value.endsWith(")")) {
            value = value.slice(1, -1).trim();
        }
        return value;
    }
}
exports.MssqlDbParser = MssqlDbParser;
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZGIucGFyc2VyLm1zc3FsLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vc3JjL3V0aWxzL2RiLnBhcnNlci5tc3NxbC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7QUFBQSxtQ0FBcUM7QUFHckMsMkNBQXVDO0FBRXZDLE1BQU0sdUJBQXVCLEdBQUcsQ0FBQyxXQUFXLEVBQUUsWUFBWSxFQUFFLFdBQVcsRUFBRSxZQUFZLENBQUMsQ0FBQztBQUV2Rjs7Ozs7O0dBTUc7QUFDSCxNQUFhLGFBQWMsU0FBUSxvQkFBUTtJQUl2QyxZQUFZLFNBQXNCLEVBQUUsTUFBZTtRQUMvQyxLQUFLLENBQUMsU0FBUyxDQUFDLENBQUM7UUFDakIsSUFBSSxDQUFDLE1BQU0sR0FBRyxNQUFNLElBQUksS0FBSyxDQUFDO0lBQ2xDLENBQUM7SUFFUSxLQUFLLENBQUMsU0FBUztRQUNwQixNQUFNLENBQUMsT0FBTyxFQUFFLFVBQVUsQ0FBQyxHQUFHLE1BQU0sT0FBTyxDQUFDLEdBQUcsQ0FBQztZQUM1QyxJQUFBLFlBQUcsRUFBSzs7Ozs7Ozs7Ozs7Ozt5Q0FhcUIsSUFBSSxDQUFDLE1BQU07O2FBRXZDLENBQUMsT0FBTyxDQUFDLElBQUksQ0FBQyxNQUFNLENBQUM7WUFDdEIsSUFBQSxZQUFHLEVBQUs7Ozs7Ozs7OzsyQ0FTdUIsSUFBSSxDQUFDLE1BQU07O2FBRXpDLENBQUMsT0FBTyxDQUFDLElBQUksQ0FBQyxNQUFNLENBQUM7U0FDekIsQ0FBQyxDQUFDO1FBRUgsTUFBTSxTQUFTLEdBQUcsSUFBSSxHQUFHLEVBQVUsQ0FBQztRQUNwQyxNQUFNLFNBQVMsR0FBRyxJQUFJLEdBQUcsRUFBVSxDQUFDO1FBQ3BDLE1BQU0sYUFBYSxHQUFHLElBQUksR0FBRyxFQUFVLENBQUM7UUFDeEMsVUFBVSxDQUFDLElBQUksQ0FBQyxPQUFPLENBQUMsQ0FBQyxDQUFDLEVBQUUsRUFBRTtZQUMxQixNQUFNLEdBQUcsR0FBRyxHQUFHLENBQUMsQ0FBQyxVQUFVLElB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