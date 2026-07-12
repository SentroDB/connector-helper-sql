import { sql } from "kysely";
import DBManagerTypes, { ColumnGeneratedType } from "@sentrodb/connector-node-types";

import { DbParser } from "./db.parser";

const UPDATED_AT_COLUMN_NAMES = ["updatedAt", "updated_at", "createdAt", "created_at"];

/**
 * MySQL flavour of the schema parser. Reads information_schema scoped to the
 * connected database (DATABASE()) and maps the results into the same
 * DBManagerTypes shapes the Postgres parser produces. All shared logic
 * (getSchemaDetails, Prisma implicit M2M folding) is inherited from DbParser.
 */
export class MysqlDbParser extends DbParser {

    override async getTables(): Promise<DBManagerTypes.Table[]> {
        const [columns, foreignKeyColumns] = await Promise.all([
            sql<any>`
                SELECT
                    c.TABLE_NAME      AS table_name,
                    c.COLUMN_NAME     AS name,
                    c.DATA_TYPE       AS type,
                    c.COLUMN_TYPE     AS column_type,
                    c.IS_NULLABLE     AS is_nullable,
                    c.COLUMN_DEFAULT  AS column_default,
                    c.COLUMN_KEY      AS column_key,
                    c.EXTRA           AS extra
                FROM information_schema.columns c
                WHERE c.TABLE_SCHEMA = DATABASE()
                ORDER BY c.TABLE_NAME, c.ORDINAL_POSITION
            `.execute(this.client),
            sql<any>`
                SELECT
                    kcu.TABLE_NAME  AS table_name,
                    kcu.COLUMN_NAME AS column_name
                FROM information_schema.key_column_usage kcu
                WHERE kcu.TABLE_SCHEMA = DATABASE()
                  AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
            `.execute(this.client),
        ]);

        const fkColumns = new Set(
            foreignKeyColumns.rows.map((r) => `${r.table_name}.${r.column_name}`),
        );

        const tables: DBManagerTypes.Table[] = [];

        columns.rows.forEach((row) => {
            const {
                table_name,
                name,
                type,
                column_type,
                is_nullable,
                column_default,
                column_key,
                extra,
            } = row;

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

            const extraText = String(extra ?? "").toLowerCase();
            const primary_key = column_key === "PRI";
            const foreign_key = fkColumns.has(`${table_name}.${name}`);
            const unique = column_key === "UNI" || primary_key;
            const autoincrement = extraText.includes("auto_increment");
            const isTextual = /char|text/i.test(String(type ?? ""));
            const isUpdatedAt =
                UPDATED_AT_COLUMN_NAMES.includes(name) &&
                /^(datetime|timestamp)$/i.test(String(type ?? ""));
            const hasOnUpdateNow = extraText.includes("on update current_timestamp");

            let generatedType: ColumnGeneratedType = "none";
            if (autoincrement) {
                generatedType = "sequence";
            } else if (String(column_default ?? "").toLowerCase().includes("uuid")) {
                generatedType = "uuid";
            } else if (primary_key && isTextual && !column_default) {
                // Prisma cuid ids are client-generated string PKs with no DB default
                generatedType = "cuid";
            }

            table.columns.push({
                name,
                type,
                nullable: is_nullable === "YES",
                default: column_default ?? ((isUpdatedAt || hasOnUpdateNow) ? "CURRENT_TIMESTAMP" : null),
                primary_key,
                foreign_key,
                unique,
                autoincrement,
                isGenerated: generatedType !== "none",
                generatedType,
                enum_values: type === "enum" ? this.parseEnumValues(column_type) : [],
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
        const [foreignKeys, uniqueColumns] = await Promise.all([
            sql<any>`
                SELECT
                    kcu.TABLE_NAME             AS table_name,
                    kcu.CONSTRAINT_NAME        AS name,
                    kcu.COLUMN_NAME            AS column_name,
                    kcu.REFERENCED_TABLE_NAME  AS reference_table,
                    kcu.REFERENCED_COLUMN_NAME AS reference_column,
                    rc.UPDATE_RULE             AS update_rule,
                    rc.DELETE_RULE             AS delete_rule
                FROM information_schema.key_column_usage kcu
                JOIN information_schema.referential_constraints rc
                    ON rc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                   AND rc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA
                WHERE kcu.TABLE_SCHEMA = DATABASE()
                  AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
                ORDER BY kcu.TABLE_NAME, kcu.CONSTRAINT_NAME
            `.execute(this.client),
            sql<any>`
                SELECT
                    s.TABLE_NAME  AS table_name,
                    s.COLUMN_NAME AS column_name
                FROM information_schema.statistics s
                WHERE s.TABLE_SCHEMA = DATABASE()
                  AND s.NON_UNIQUE = 0
            `.execute(this.client),
        ]);

        const uniqueSet = new Set(
            uniqueColumns.rows.map((r) => `${r.table_name}.${r.column_name}`),
        );

        const constraints: DBManagerTypes.Constraint[] = [];

        foreignKeys.rows.forEach((row) => {
            const { table_name, name, column_name, reference_table, reference_column, update_rule, delete_rule } =
                row;

            const isUnique = uniqueSet.has(`${table_name}.${column_name}`);
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
                s.INDEX_NAME  AS index_name,
                s.TABLE_NAME  AS table_name,
                s.COLUMN_NAME AS column_name,
                CASE WHEN s.INDEX_NAME = 'PRIMARY' THEN 1 ELSE 0 END AS is_primary,
                CASE WHEN s.NON_UNIQUE = 0 THEN 1 ELSE 0 END AS is_unique
            FROM information_schema.statistics s
            WHERE s.TABLE_SCHEMA = DATABASE()
            ORDER BY s.TABLE_NAME, s.INDEX_NAME, s.SEQ_IN_INDEX
        `.execute(this.client);

        const indexes: DBManagerTypes.Index[] = [];

        indexList.rows.forEach((row) => {
            const { table_name, index_name, column_name, is_primary, is_unique } = row;

            // MySQL index names are only unique per table (every PK is "PRIMARY"),
            // so dedupe by table + name rather than name alone.
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

    private parseEnumValues(columnType: string): string[] {
        // COLUMN_TYPE looks like: enum('a','b','c')
        const match = /^enum\((.*)\)$/i.exec(String(columnType ?? ""));
        if (!match) return [];
        return match[1]
            .split(",")
            .map((v) => v.trim())
            .map((v) => v.replace(/^'(.*)'$/, "$1").replace(/''/g, "'"));
    }
}
