"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.MysqlDbParser = void 0;
const kysely_1 = require("kysely");
const db_parser_1 = require("./db.parser");
const UPDATED_AT_COLUMN_NAMES = ["updatedAt", "updated_at", "createdAt", "created_at"];
/**
 * MySQL flavour of the schema parser. Reads information_schema scoped to the
 * connected database (DATABASE()) and maps the results into the same
 * DBManagerTypes shapes the Postgres parser produces. All shared logic
 * (getSchemaDetails, Prisma implicit M2M folding) is inherited from DbParser.
 */
class MysqlDbParser extends db_parser_1.DbParser {
    async getTables() {
        const [columns, foreignKeyColumns] = await Promise.all([
            (0, kysely_1.sql) `
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
            (0, kysely_1.sql) `
                SELECT
                    kcu.TABLE_NAME  AS table_name,
                    kcu.COLUMN_NAME AS column_name
                FROM information_schema.key_column_usage kcu
                WHERE kcu.TABLE_SCHEMA = DATABASE()
                  AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
            `.execute(this.client),
        ]);
        const fkColumns = new Set(foreignKeyColumns.rows.map((r) => `${r.table_name}.${r.column_name}`));
        const tables = [];
        columns.rows.forEach((row) => {
            const { table_name, name, type, column_type, is_nullable, column_default, column_key, extra, } = row;
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
            const isUpdatedAt = UPDATED_AT_COLUMN_NAMES.includes(name) &&
                /^(datetime|timestamp)$/i.test(String(type ?? ""));
            const hasOnUpdateNow = extraText.includes("on update current_timestamp");
            let generatedType = "none";
            if (autoincrement) {
                generatedType = "sequence";
            }
            else if (String(column_default ?? "").toLowerCase().includes("uuid")) {
                generatedType = "uuid";
            }
            else if (primary_key && isTextual && !column_default) {
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
    async getConstraints() {
        const [foreignKeys, uniqueColumns] = await Promise.all([
            (0, kysely_1.sql) `
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
            (0, kysely_1.sql) `
                SELECT
                    s.TABLE_NAME  AS table_name,
                    s.COLUMN_NAME AS column_name
                FROM information_schema.statistics s
                WHERE s.TABLE_SCHEMA = DATABASE()
                  AND s.NON_UNIQUE = 0
            `.execute(this.client),
        ]);
        const uniqueSet = new Set(uniqueColumns.rows.map((r) => `${r.table_name}.${r.column_name}`));
        const constraints = [];
        foreignKeys.rows.forEach((row) => {
            const { table_name, name, column_name, reference_table, reference_column, update_rule, delete_rule } = row;
            const isUnique = uniqueSet.has(`${table_name}.${column_name}`);
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
                s.INDEX_NAME  AS index_name,
                s.TABLE_NAME  AS table_name,
                s.COLUMN_NAME AS column_name,
                CASE WHEN s.INDEX_NAME = 'PRIMARY' THEN 1 ELSE 0 END AS is_primary,
                CASE WHEN s.NON_UNIQUE = 0 THEN 1 ELSE 0 END AS is_unique
            FROM information_schema.statistics s
            WHERE s.TABLE_SCHEMA = DATABASE()
            ORDER BY s.TABLE_NAME, s.INDEX_NAME, s.SEQ_IN_INDEX
        `.execute(this.client);
        const indexes = [];
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
    parseEnumValues(columnType) {
        // COLUMN_TYPE looks like: enum('a','b','c')
        const match = /^enum\((.*)\)$/i.exec(String(columnType ?? ""));
        if (!match)
            return [];
        return match[1]
            .split(",")
            .map((v) => v.trim())
            .map((v) => v.replace(/^'(.*)'$/, "$1").replace(/''/g, "'"));
    }
}
exports.MysqlDbParser = MysqlDbParser;
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZGIucGFyc2VyLm15c3FsLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vc3JjL3V0aWxzL2RiLnBhcnNlci5teXNxbC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7QUFBQSxtQ0FBNkI7QUFHN0IsMkNBQXVDO0FBRXZDLE1BQU0sdUJBQXVCLEdBQUcsQ0FBQyxXQUFXLEVBQUUsWUFBWSxFQUFFLFdBQVcsRUFBRSxZQUFZLENBQUMsQ0FBQztBQUV2Rjs7Ozs7R0FLRztBQUNILE1BQWEsYUFBYyxTQUFRLG9CQUFRO0lBRTlCLEtBQUssQ0FBQyxTQUFTO1FBQ3BCLE1BQU0sQ0FBQyxPQUFPLEVBQUUsaUJBQWlCLENBQUMsR0FBRyxNQUFNLE9BQU8sQ0FBQyxHQUFHLENBQUM7WUFDbkQsSUFBQSxZQUFHLEVBQUs7Ozs7Ozs7Ozs7Ozs7YUFhUCxDQUFDLE9BQU8sQ0FBQyxJQUFJLENBQUMsTUFBTSxDQUFDO1lBQ3RCLElBQUEsWUFBRyxFQUFLOzs7Ozs7O2FBT1AsQ0FBQyxPQUFPLENBQUMsSUFBSSxDQUFDLE1BQU0sQ0FBQztTQUN6QixDQUFDLENBQUM7UUFFSCxNQUFNLFNBQVMsR0FBRyxJQUFJLEdBQUcsQ0FDckIsaUJBQWlCLENBQUMsSUFBSSxDQUFDLEdBQUcsQ0FBQyxDQUFDLENBQUMsRUFBRSxFQUFFLENBQUMsR0FBRyxDQUFDLENBQUMsVUFBVSxJQUFJLENBQUMsQ0FBQyxXQUFXLEVBQUUsQ0FBQyxDQUN4RSxDQUFDO1FBRUYsTUFBTSxNQUFNLEdBQTJCLEVBQUUsQ0FBQztRQUUxQyxPQUFPLENBQUMsSUFBSSxDQUFDLE9BQU8sQ0FBQyxDQUFDLEdBQUcsRUFBRSxFQUFFO1lBQ3pCLE1BQU0sRUFDRixVQUFVLEVBQ1YsSUFBSSxFQUNKLElBQUksRUFDSixXQUFXLEVBQ1gsV0FBVyxFQUNYLGNBQWMsRUFDZCxVQUFVLEVBQ1YsS0FBSyxHQUNSLEdBQUcsR0FBRyxDQUFDO1lBRVIsSUFBSSxLQUFLLEdBQUcsTUFBTSxDQUFDLElB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