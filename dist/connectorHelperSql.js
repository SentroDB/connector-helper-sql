"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const kysely_1 = require("kysely");
const mysql_1 = __importDefault(require("./connectors/mysql"));
const postgre_1 = __importDefault(require("./connectors/postgre"));
const mssql_1 = __importDefault(require("./connectors/mssql"));
const db_parser_1 = require("./utils/db.parser");
const db_parser_mysql_1 = require("./utils/db.parser.mysql");
const db_parser_mssql_1 = require("./utils/db.parser.mssql");
/** Cast a column reference to a string type for text search, per dialect. */
function castToText(ref, dialect) {
    if (dialect === "mysql")
        return (0, kysely_1.sql) `CAST(${ref} AS CHAR)`;
    if (dialect === "mssql")
        return (0, kysely_1.sql) `CAST(${ref} AS NVARCHAR(MAX))`;
    return (0, kysely_1.sql) `${ref}::text`;
}
/**
 * Case-insensitive LIKE that works across dialects. Postgres and MySQL
 * default the LIKE escape character to backslash; MSSQL has no default
 * escape character, so it needs an explicit ESCAPE clause.
 */
function caseInsensitiveLike(ref, pattern, dialect) {
    const target = castToText(ref, dialect);
    if (dialect === "mssql") {
        return (0, kysely_1.sql) `LOWER(${target}) LIKE LOWER(${pattern}) ESCAPE '\\'`;
    }
    return (0, kysely_1.sql) `LOWER(${target}) LIKE LOWER(${pattern})`;
}
/**
 * Apply a list of segment-style structured conditions to a Kysely query.
 * Conditions are AND-merged. Unknown operators are ignored.
 */
function applySegmentConditions(query, conditions, dialect, alias = "t") {
    if (!conditions?.length)
        return query;
    let q = query;
    for (const cond of conditions) {
        if (!cond?.column || !cond.operator)
            continue;
        const ref = kysely_1.sql.ref(`${alias}.${cond.column}`);
        switch (cond.operator) {
            case "eq":
                q = q.where(ref, "=", cond.value);
                break;
            case "neq":
                q = q.where(ref, "!=", cond.value);
                break;
            case "gt":
                q = q.where(ref, ">", cond.value);
                break;
            case "gte":
                q = q.where(ref, ">=", cond.value);
                break;
            case "lt":
                q = q.where(ref, "<", cond.value);
                break;
            case "lte":
                q = q.where(ref, "<=", cond.value);
                break;
            case "contains":
                q = q.where(caseInsensitiveLike(ref, `%${String(cond.value)}%`, dialect));
                break;
            case "startsWith":
                q = q.where(caseInsensitiveLike(ref, `${String(cond.value)}%`, dialect));
                break;
            case "endsWith":
                q = q.where(caseInsensitiveLike(ref, `%${String(cond.value)}`, dialect));
                break;
            case "in":
                if (Array.isArray(cond.value) && cond.value.length) {
                    q = q.where(ref, "in", cond.value);
                }
                break;
            case "notIn":
                if (Array.isArray(cond.value) && cond.value.length) {
                    q = q.where(ref, "not in", cond.value);
                }
                break;
            case "isNull":
                q = q.where(ref, "is", null);
                break;
            case "isNotNull":
                q = q.where(ref, "is not", null);
                break;
        }
    }
    return q;
}
class ConnectorHelperSql {
    get dialect() {
        return this.dbConfig?.type ?? "postgres";
    }
    /**
     * OR-merged case-insensitive LIKE clause over the given columns, with
     * LIKE wildcards in the search term escaped.
     */
    buildSearchClause(search, searchColumns) {
        const escaped = search.replace(/[\\%_]/g, "\\$&");
        const likePattern = `%${escaped}%`;
        return searchColumns
            .map((col) => caseInsensitiveLike(kysely_1.sql.ref(`t.${col}`), likePattern, this.dialect))
            .reduce((acc, piece) => (acc ? (0, kysely_1.sql) `${acc} OR ${piece}` : piece), undefined);
    }
    async connect({ config }) {
        this.dbConfig = config;
        if (config.type == "postgres") {
            this.dbHandler = (0, postgre_1.default)(config);
        }
        else if (config.type == "mysql") {
            this.dbHandler = (0, mysql_1.default)(config);
        }
        else if (config.type == "mssql") {
            this.dbHandler = (0, mssql_1.default)(config);
        }
    }
    async disconnect() {
        if (this.dbHandler) {
            await this.dbHandler.destroy();
            this.dbHandler = undefined;
        }
    }
    getSchemaDetails() {
        if (!this.dbHandler)
            throw new Error("Database handler is not initialized.");
        if (this.dbConfig?.type === "mysql") {
            return (new db_parser_mysql_1.MysqlDbParser(this.dbHandler)).getSchemaDetails();
        }
        if (this.dbConfig?.type === "mssql") {
            return (new db_parser_mssql_1.MssqlDbParser(this.dbHandler, this.dbConfig.schema)).getSchemaDetails();
        }
        return (new db_parser_1.DbParser(this.dbHandler)).getSchemaDetails();
    }
    async get({ table, where, limit, offset, orderBy, orderDirection, search, searchColumns, columns, extraConditions }) {
        if (!this.dbHandler)
            return;
        if (!table || typeof table !== "string") {
            throw new Error(`ConnectorHelperSql.get: invalid "table" argument: ${String(table)}`);
        }
        const t = kysely_1.sql.table(table).as("t");
        let query = this.dbHandler.selectFrom(t);
        if (columns?.length)
            query = query.select(columns.map((col) => kysely_1.sql.ref(`t.${col}`)));
        else
            query = query.selectAll("t");
        if (where && Object.keys(where).length) {
            for (const [col, val] of Object.entries(where)) {
                query = query.where(kysely_1.sql.ref(`t.${col}`), "=", val);
            }
        }
        query = applySegmentConditions(query, extraConditions, this.dialect);
        if (search && searchColumns?.length) {
            const clause = this.buildSearchClause(search, searchColumns);
            if (clause) {
                query = query.where(clause);
            }
        }
        if (orderBy) {
            query = query.orderBy(kysely_1.sql.ref(`t.${orderBy}`), orderDirection ?? "asc");
        }
        else if (this.dialect === "mssql" && (typeof limit === "number" || typeof offset === "number")) {
            // MSSQL compiles limit/offset to OFFSET ... FETCH, which is a
            // syntax error without an ORDER BY. Inject the standard no-op.
            query = query.orderBy((0, kysely_1.sql) `(SELECT NULL)`);
        }
        if (typeof limit === "number")
            query = query.limit(limit);
        if (typeof offset === "number")
            query = query.offset(offset);
        return query.execute();
    }
    async getSingle({ table, where }) {
        if (!this.dbHandler)
            return;
        if (!table || typeof table !== "string") {
            throw new Error(`ConnectorHelperSql.getSingle: invalid "table" argument: ${String(table)}`);
        }
        const t = kysely_1.sql.table(table).as("t");
        let query = this.dbHandler.selectFrom(t).selectAll("t");
        if (where && Object.keys(where).length) {
            for (const [col, val] of Object.entries(where)) {
                query = query.where(kysely_1.sql.ref(`t.${col}`), "=", val);
            }
        }
        return query.executeTakeFirst();
    }
    async update({ table, data, where, junctions }) {
        if (!this.dbHandler)
            return;
        if (!where || !Object.keys(where).length) {
            throw new Error(`Refusing to update "${table}" without a where clause — this would affect every row.`);
        }
        if (!junctions?.length) {
            let query = this.dbHandler.updateTable(table).set(data);
            query = query.where((eb) => eb.and(where));
            return query.execute();
        }
        return this.dbHandler.transaction().execute(async (trx) => {
            let q = trx.updateTable(table).set(data);
            if (where)
                q = q.where((eb) => eb.and(where));
            const result = await q.execute();
            const sourceValues = await this.collectSourceValues(trx, table, where, junctions);
            for (const j of junctions) {
                if (!sourceValues.length)
                    continue;
                await trx
                    .deleteFrom(j.junctionTable)
                    .where(j.sourceColumn, "in", sourceValues)
                    .execute();
                const rows = [];
                for (const sv of sourceValues) {
                    for (const tid of j.targetIds) {
                        rows.push({ [j.sourceColumn]: sv, [j.targetColumn]: tid });
                    }
                }
                if (rows.length) {
                    await trx.insertInto(j.junctionTable).values(rows).execute();
                }
            }
            return result;
        });
    }
    async insert({ table, data, junctions }) {
        if (!this.dbHandler)
            return;
        if (!junctions?.length) {
            return this.dbHandler.insertInto(table).values(data).execute();
        }
        return this.dbHandler.transaction().execute(async (trx) => {
            const inserted = await this.insertReturning(trx, table, data);
            for (const j of junctions) {
                const rows = [];
                for (const parent of inserted) {
                    // Fall back to the MySQL auto-increment id when the PK
                    // wasn't part of the input data (see insertReturning).
                    const sourceValue = parent[j.parentSourceColumn] ?? parent.insertId;
                    if (sourceValue == null)
                        continue;
                    for (const tid of j.targetIds) {
                        rows.push({ [j.sourceColumn]: sourceValue, [j.targetColumn]: tid });
                    }
                }
                if (rows.length) {
                    await trx.insertInto(j.junctionTable).values(rows).execute();
                }
            }
            return inserted;
        });
    }
    /**
     * Insert rows and return the inserted rows, per dialect:
     * - postgres: RETURNING *
     * - mssql: OUTPUT inserted.*
     * - mysql: no RETURNING support — reconstruct rows from the input data,
     *   attaching the auto-increment id as `insertId` when the DB generated
     *   one (MySQL returns the first id of a multi-row batch; subsequent rows
     *   are consecutive).
     */
    async insertReturning(trx, table, data) {
        if (this.dialect === "mssql") {
            return trx.insertInto(table).values(data).outputAll("inserted").execute();
        }
        if (this.dialect === "mysql") {
            const result = await trx.insertInto(table).values(data).executeTakeFirst();
            const rows = Array.isArray(data) ? data : [data];
            const insertId = result?.insertId;
            return rows.map((row, i) => insertId != null ? { insertId: Number(insertId) + i, ...row } : { ...row });
        }
        return trx.insertInto(table).values(data).returningAll().execute();
    }
    /**
     * Read target ids from a junction table for a given source row. Used by
     * the connector to populate M2M multi-selects with current selections.
     */
    async getRelatedIds({ junctionTable, sourceColumn, sourceValue, targetColumn, }) {
        if (!this.dbHandler)
            return [];
        const rows = await this.dbHandler
            .selectFrom(junctionTable)
            .select(targetColumn)
            .where(sourceColumn, "=", sourceValue)
            .execute();
        return rows.map((r) => r[targetColumn]);
    }
    async collectSourceValues(trx, table, where, junctions) {
        const cols = Array.from(new Set(junctions.map((j) => j.parentSourceColumn)));
        if (!cols.length)
            return [];
        let q = trx.selectFrom(table).select(cols);
        if (where)
            q = q.where((eb) => eb.and(where));
        const rows = await q.execute();
        // Single source column case (the common one) — flatten to array of values.
        if (cols.length === 1) {
            return rows.map((r) => r[cols[0]]).filter((v) => v != null);
        }
        // Multi-source case shouldn't happen with Prisma implicit M2M (always PK),
        // but be safe: only return if all junctions share the same source column.
        return rows.map((r) => r[cols[0]]).filter((v) => v != null);
    }
    async delete({ table, where, single }) {
        if (!this.dbHandler)
            return;
        if (!where || typeof where !== "object" || !Object.keys(where).length) {
            throw new Error(`Refusing to delete from "${table}" without a where clause — this would affect every row.`);
        }
        let query = this.dbHandler.deleteFrom(table);
        if (single) {
            query = query.where(where);
            return query.executeTakeFirst();
        }
        query = query.where((eb) => eb.and(Object.entries(where).map(([col, val]) => {
            return eb.or(val.map((v) => eb(col, "=", v)));
        })));
        return query.execute();
    }
    async count({ table, where, search, searchColumns, extraConditions, }) {
        if (!this.dbHandler)
            return;
        if (!table || typeof table !== "string") {
            throw new Error(`ConnectorHelperSql.count: invalid "table" argument: ${String(table)}`);
        }
        const t = kysely_1.sql.table(table).as("t");
        let q = this.dbHandler
            .selectFrom(t)
            .select(({ fn }) => fn.countAll().as("count"));
        if (where && Object.keys(where).length) {
            for (const [col, val] of Object.entries(where)) {
                q = q.where(kysely_1.sql.ref(`t.${col}`), "=", val);
            }
        }
        q = applySegmentConditions(q, extraConditions, this.dialect);
        if (search && searchColumns?.length) {
            const orClause = this.buildSearchClause(search, searchColumns);
            if (orClause) {
                q = q.where(orClause);
            }
        }
        return q.execute();
    }
    async query({ sql: rawSql, params, schema }) {
        if (!this.dbHandler)
            throw new Error("Database handler is not initialized.");
        if (schema) {
            if (!/^[A-Za-z_][A-Za-z0-9_$]*$/.test(schema)) {
                throw new Error(`ConnectorHelperSql.query: invalid "schema" argument: ${String(schema)}`);
            }
            if (this.dialect === "postgres") {
                await this.dbHandler.executeQuery(kysely_1.CompiledQuery.raw(`SET search_path TO "${schema}"`));
            }
            else if (this.dialect === "mysql") {
                // In MySQL a schema is a database
                await this.dbHandler.executeQuery(kysely_1.CompiledQuery.raw(`USE \`${schema}\``));
            }
            else {
                // MSSQL has no session-level default-schema switch
                throw new Error(`ConnectorHelperSql.query: schema switching is not supported on ${this.dialect}; qualify table names instead.`);
            }
        }
        const compiled = kysely_1.CompiledQuery.raw(rawSql, params ?? []);
        const result = await this.dbHandler.executeQuery(compiled);
        const rows = result.rows ?? [];
        const columns = rows.length > 0 ? Object.keys(rows[0]) : [];
        return { rows, columns };
    }
}
exports.default = ConnectorHelperSql;
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiY29ubmVjdG9ySGVscGVyU3FsLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vc3JjL2Nvbm5lY3RvckhlbHBlclNxbC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7OztBQUFBLG1DQUFvRDtBQUVwRCwrREFBZ0Q7QUFDaEQsbUVBQXFEO0FBQ3JELCtEQUFnRDtBQUNoRCxpREFBNkM7QUFDN0MsNkRBQXdEO0FBQ3hELDZEQUF3RDtBQWtCeEQsNkVBQTZFO0FBQzdFLFNBQVMsVUFBVSxDQUFDLEdBQVEsRUFBRSxPQUFtQjtJQUM3QyxJQUFJLE9BQU8sS0FBSyxPQUFPO1FBQUUsT0FBTyxJQUFBLFlBQUcsRUFBQSxRQUFRLEdBQUcsV0FBVyxDQUFDO0lBQzFELElBQUksT0FBTyxLQUFLLE9BQU87UUFBRSxPQUFPLElBQUEsWUFBRyxFQUFBLFFBQVEsR0FBRyxvQkFBb0IsQ0FBQztJQUNuRSxPQUFPLElBQUEsWUFBRyxFQUFBLEdBQUcsR0FBRyxRQUFRLENBQUM7QUFDN0IsQ0FBQztBQUVEOzs7O0dBSUc7QUFDSCxTQUFTLG1CQUFtQixDQUFDLEdBQVEsRUFBRSxPQUFlLEVBQUUsT0FBbUI7SUFDdkUsTUFBTSxNQUFNLEdBQUcsVUFBVSxDQUFDLEdBQUcsRUFBRSxPQUFPLENBQUMsQ0FBQztJQUN4QyxJQUFJLE9BQU8sS0FBSyxPQUFPLEVBQUUsQ0FBQztRQUN0QixPQUFPLElBQUEsWUFBRyxFQUFBLFNBQVMsTUFBTSxnQkFBZ0IsT0FBTyxlQUFlLENBQUM7SUFDcEUsQ0FBQztJQUNELE9BQU8sSUFBQSxZQUFHLEVBQUEsU0FBUyxNQUFNLGdCQUFnQixPQUFPLEdBQUcsQ0FBQztBQUN4RCxDQUFDO0FBRUQ7OztHQUdHO0FBQ0gsU0FBUyxzQkFBc0IsQ0FDM0IsS0FBUSxFQUNSLFVBQTBDLEVBQzFDLE9BQW1CLEVBQ25CLEtBQUssR0FBRyxHQUFHO0lBRVgsSUFBSSxDQUFDLFVBQVUsRUFBRSxNQUFNO1FBQUUsT0FBTyxLQUFLLENBQUM7SUFD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