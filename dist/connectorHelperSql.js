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
/**
 * Apply a list of segment-style structured conditions to a Kysely query.
 * Conditions are AND-merged. Unknown operators are ignored.
 */
function applySegmentConditions(query, conditions, alias = "t") {
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
                q = q.where((0, kysely_1.sql) `LOWER(${ref}::text) LIKE LOWER(${`%${String(cond.value)}%`})`);
                break;
            case "startsWith":
                q = q.where((0, kysely_1.sql) `LOWER(${ref}::text) LIKE LOWER(${`${String(cond.value)}%`})`);
                break;
            case "endsWith":
                q = q.where((0, kysely_1.sql) `LOWER(${ref}::text) LIKE LOWER(${`%${String(cond.value)}`})`);
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
        query = applySegmentConditions(query, extraConditions);
        if (search && searchColumns?.length) {
            const escaped = search.replace(/[%_]/g, "\\$&");
            const likePattern = `%${escaped}%`;
            const clause = searchColumns
                .map((col) => (0, kysely_1.sql) `LOWER(${kysely_1.sql.ref(`t.${col}`)}::text) LIKE LOWER(${likePattern})`)
                .reduce((acc, piece) => (acc ? (0, kysely_1.sql) `${acc} OR ${piece}` : piece), undefined);
            if (clause) {
                query = query.where(clause);
            }
        }
        if (orderBy) {
            query = query.orderBy(kysely_1.sql.ref(`t.${orderBy}`), orderDirection ?? "asc");
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
    async update({ table, data, where }) {
        if (!this.dbHandler)
            return;
        const query = this.dbHandler.updateTable(table).set(data);
        if (where)
            query.where((eb) => eb.and(where));
        return query.execute();
    }
    async insert({ table, data }) {
        if (!this.dbHandler)
            return;
        const query = this.dbHandler.insertInto(table).values(data);
        return query.execute();
    }
    async delete({ table, where, single }) {
        if (!this.dbHandler)
            return;
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
        q = applySegmentConditions(q, extraConditions);
        if (search && searchColumns?.length) {
            const escaped = search.replace(/[%_]/g, "\\$&");
            const likePattern = `%${escaped}%`;
            const orClause = searchColumns
                .map((col) => (0, kysely_1.sql) `LOWER(${kysely_1.sql.ref(`t.${col}`)}::text) LIKE LOWER(${likePattern})`)
                .reduce((acc, piece) => (acc ? (0, kysely_1.sql) `${acc} OR ${piece}` : piece), undefined);
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
            await this.dbHandler.executeQuery(kysely_1.CompiledQuery.raw(`SET search_path TO "${schema}"`));
        }
        const compiled = kysely_1.CompiledQuery.raw(rawSql, params ?? []);
        const result = await this.dbHandler.executeQuery(compiled);
        const rows = result.rows ?? [];
        const columns = rows.length > 0 ? Object.keys(rows[0]) : [];
        return { rows, columns };
    }
}
exports.default = ConnectorHelperSql;
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiY29ubmVjdG9ySGVscGVyU3FsLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vc3JjL2Nvbm5lY3RvckhlbHBlclNxbC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7OztBQUFBLG1DQUFvRDtBQUVwRCwrREFBZ0Q7QUFDaEQsbUVBQXFEO0FBQ3JELCtEQUFnRDtBQUNoRCxpREFBNkM7QUFHN0M7OztHQUdHO0FBQ0gsU0FBUyxzQkFBc0IsQ0FDM0IsS0FBUSxFQUNSLFVBQTBDLEVBQzFDLEtBQUssR0FBRyxHQUFHO0lBRVgsSUFBSSxDQUFDLFVBQVUsRUFBRSxNQUFNO1FBQUUsT0FBTyxLQUFLLENBQUM7SUFDdEMsSUFBSSxDQUFDLEdBQVEsS0FBSyxDQUFDO0lBQ25CLEtBQUssTUFBTSxJQUFJLElBQUksVUFBVSxFQUFFLENBQUM7UUFDNUIsSUFBSSxDQUFDLElBQUksRUFBRSxNQUFNLElBQUksQ0FBQyxJQUFJLENBQUMsUUFBUTtZQUFFLFNBQVM7UUFDOUMsTUFBTSxHQUFHLEdBQUcsWUFBRyxDQUFDLEdBQUcsQ0FBQyxHQUFHLEtBQUssSUFBSSxJQUFJLENBQUMsTUFBTSxFQUFFLENBQUMsQ0FBQztRQUMvQyxRQUFRLElBQUksQ0FBQyxRQUFRLEVBQUUsQ0FBQztZQUNwQixLQUFLLElBQUk7Z0JBQ0wsQ0FBQyxHQUFHLENBQUMsQ0FBQyxLQUFLLENBQUMsR0FBRyxFQUFFLEdBQUcsRUFBRSxJQUFJLENBQUMsS0FBWSxDQUFDLENBQUM7Z0JBQ3pDLE1BQU07WUFDVixLQUFLLEtBQUs7Z0JBQ04sQ0FBQyxHQUFHLENBQUMsQ0FBQyxLQUFLLENBQUMsR0FBRyxFQUFFLElBQUksRUFBRSxJQUFJLENBQUMsS0FBWSxDQUFDLENBQUM7Z0JBQzFDLE1BQU07WUFDVixLQUFLLElBQUk7Z0JBQ0wsQ0FBQyxHQUFHLENBQUMsQ0FBQyxLQUFLLENBQUMsR0FBRyxFQUFFLEdBQUcsRUFBRSxJQUFJLENBQUMsS0FBWSxDQUFDLENBQUM7Z0JBQ3pDLE1B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