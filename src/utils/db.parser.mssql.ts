import { Kysely, sql } from "kysely";
import DBManagerTypes, { ColumnGeneratedType } from "@sentrodb/connector-node-types";

import { DbParser } from "./db.parser";

const UPDATED_AT_COLUMN_NAMES = ["updatedAt", "updated_at", "createdAt", "created_at"];

/**
 * SQL Server flavour of the schema parser. Reads INFORMATION_SCHEMA plus the
 * sys.* catalog views (FKs and indexes are not reliably exposed through
 * INFORMATION_SCHEMA on MSSQL) and maps the results into the same
 * DBManagerTypes shapes the Postgres parser produces. All shared logic
 * (getSchemaDetails, Prisma implicit M2M folding) is inherited from DbParser.
 */
export class MssqlDbParser extends DbParser {

    private schema: string;

    constructor(dbHandler: Kysely<any>, schema?: string) {
        super(dbHandler);
        this.schema = schema || "dbo";
    }

    override async getTables(): Promise<DBManagerTypes.Table[]> {
        const [columns, keyColumns] = await Promise.all([
            sql<any>`
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
            sql<any>`
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

        const pkColumns = new Set<string>();
        const fkColumns = new Set<string>();
        const uniqueColumns = new Set<string>();
        keyColumns.rows.forEach((r) => {
            const key = `${r.table_name}.${r.column_name}`;
            if (r.constraint_type === "PRIMARY KEY") pkColumns.add(key);
            else if (r.constraint_type === "FOREIGN KEY") fkColumns.add(key);
            else if (r.constraint_type === "UNIQUE") uniqueColumns.add(key);
        });

        const tables: DBManagerTypes.Table[] = [];

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
            const isUpdatedAt =
                UPDATED_AT_COLUMN_NAMES.includes(name) &&
                /date|timestamp/i.test(String(type ?? ""));

            let generatedType: ColumnGeneratedType = "none";
            if (autoincrement) {
                generatedType = "sequence";
            } else if (/newid|newsequentialid|uuid/i.test(String(defaultValue ?? ""))) {
                generatedType = "uuid";
            } else if (primary_key && isTextual && !defaultValue) {
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

    override async getConstraints(): Promise<DBManagerTypes.Constraint[]> {
        const foreignKeys = await sql<any>`
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

        const constraints: DBManagerTypes.Constraint[] = [];

        foreignKeys.rows.forEach((row) => {
            const { table_name, name, column_name, reference_table, reference_column, update_rule, delete_rule, is_unique_fk } =
                row;

            const isUnique = Boolean(is_unique_fk);
            const relationshipType: DBManagerTypes.RelationshipType = isUnique ? "one-to-one" : "one-to-many";

            let constraint = constraints.find((c) => c.table === table_name && c.name === name);
            if (!constraint) {
                constraint = {
                    table: table_name,
                    name,
                    column: column_name,
                    reference:
                        reference_table
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

    override async getIndexes(): Promise<DBManagerTypes.Index[]> {
        const indexList = await sql<any>`
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

        const indexes: DBManagerTypes.Index[] = [];

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
    private normalizeDefault(def: string | null): string | null {
        if (def == null) return null;
        let value = String(def).trim();
        while (value.startsWith("(") && value.endsWith(")")) {
            value = value.slice(1, -1).trim();
        }
        return value;
    }
}
