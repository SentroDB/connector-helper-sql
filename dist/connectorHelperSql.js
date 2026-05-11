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
    async update({ table, data, where, junctions }) {
        if (!this.dbHandler)
            return;
        if (!junctions?.length) {
            const query = this.dbHandler.updateTable(table).set(data);
            if (where)
                query.where((eb) => eb.and(where));
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
            const inserted = await trx
                .insertInto(table)
                .values(data)
                .returningAll()
                .execute();
            for (const j of junctions) {
                const rows = [];
                for (const parent of inserted) {
                    const sourceValue = parent[j.parentSourceColumn];
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
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiY29ubmVjdG9ySGVscGVyU3FsLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vc3JjL2Nvbm5lY3RvckhlbHBlclNxbC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7OztBQUFBLG1DQUFvRDtBQUVwRCwrREFBZ0Q7QUFDaEQsbUVBQXFEO0FBQ3JELCtEQUFnRDtBQUNoRCxpREFBNkM7QUFnQjdDOzs7R0FHRztBQUNILFNBQVMsc0JBQXNCLENBQzNCLEtBQVEsRUFDUixVQUEwQyxFQUMxQyxLQUFLLEdBQUcsR0FBRztJQUVYLElBQUksQ0FBQyxVQUFVLEVBQUUsTUFBTTtRQUFFLE9BQU8sS0FBSyxDQUFDO0lBQ3RDLElBQUksQ0FBQyxHQUFRLEtBQUssQ0FBQztJQUNuQixLQUFLLE1BQU0sSUFBSSxJQUFJLFVBQVUsRUFBRSxDQUFDO1FBQzVCLElBQUksQ0FBQyxJQUFJLEVBQUUsTUFBTSxJQUFJLENBQUMsSUFBSSxDQUFDLFFBQVE7WUFBRSxTQUFTO1FBQzlDLE1BQU0sR0FBRyxHQUFHLFlBQUcsQ0FBQyxHQUFHLENBQUMsR0FBRyxLQUFLLElBQUksSUFBSSxDQUFDLE1BQU0sRUFBRSxDQUFDLENBQUM7UUFDL0MsUUFBUSxJQUFJLENBQUMsUUFBUSxFQUFFLENBQUM7WUFDcEIsS0FBSyxJQUFJO2dCQUNMLENBQUMsR0FBRyxDQUFDLENBQUMsS0FBSyxDQUFDLEdBQUcsRUFBRSxHQUFHLEVBQUUsSUFBSSxDQUFDLEtBQVksQ0FBQyxDQUFDO2dCQUN6QyxNQUFNO1lBQ1YsS0FBSyxLQUFLO2dCQUNOLENBQUMsR0FBRyxDQUFDLENBQUMsS0FBSyxDQUFDLEdBQUcsRUFBRSxJQUFJLEVBQUUsSUFBSSxDQUFDLEtBQVksQ0FBQyxDQUFDO2dCQUMxQyxNQUFNO1lBQ1YsS0FBSyxJQUFJO2dCQUNMLENBQUMsR0FBRyxDQUFDLENBQUMsS0FBSyxDQUFDLEdBQUcsRUFBRSxHQUFHLEVBQUUsSUFBSSxDQUFDLEtBQVksQ0FBQyxDQUFDO2dCQUN6QyxNQUFNO1lBQ1YsS0FBSyxLQUFLO2dCQUNOLENBQUMsR0FBRyxDQUFDLENBQUMsS0FBSyxDQUFDLEdBQUcsRUFBRSxJQUFJLEVBQUUsSUFBSSxDQUFDLEtBQVksQ0FBQyxDQUFDO2dCQUMxQyxNQUFNO1lBQ1YsS0FBSyxJQUFJO2dCQUNMLENBQUMsR0FBRyxDQUFDLENBQUMsS0FBSyxDQUFDLEdBQUcsRUFBRSxHQUFHLEVBQUUsSUFBSSxDQUFDLEtBQVksQ0FBQyxDQUFDO2dCQUN6QyxNQUFNO1lBQ1YsS0FBSyxLQUFLO2dCQUNOLENBQUMsR0FBRyxDQUFDLENBQUMsS0FBSyxDQUFDLEdBQUcsRUFBRSxJQUFJLEVBQUUsSUFBSSxDQUFDLEtBQVksQ0FBQyxDQUFDO2dCQUMxQyxNQUFNO1lBQ1YsS0FBSyxVQUFVO2dCQUNYLENBQUMsR0FBRyxDQUFDLENBQUMsS0FBSyxDQUFDLElBQUEsWUFBRyxFQUFBLFNBQVMsR0FBRyxzQkFBc0IsSUFBSSxNQUFNLENBQUMsSUFBSSxDQUFDLEtBQUssQ0FBQyxHQUFHLEdBQUcsQ0FBQyxDQUFDO2dCQUMvRSxNQUFNO1lBQ1YsS0FBSyxZQUFZO2dCQUNiLENBQUMsR0FBRyxDQUFDLENBQUMsS0FBSyxDQUFDLElBQUEsWUFBRyxFQUFBLFNBQVMsR0FBRyxzQkFBc0IsR0FBRyxNQUFNLENBQUMsSUFBSSxDQUFDLEtBQUssQ0FBQyxHQUFHLEdBQUcsQ0FBQyxDQUFDO2dCQUM5RSxNQUFNO1lBQ1YsS0FBSyxVQUFVO2dCQUNYLENBQUMsR0FBRyxDQUFDLENBQUMsS0FBSyxDQUFDLElBQUEsWUFBRyxFQUFBLFNBQVMsR0FBRyxzQkFBc0IsSUFBSSxNQUFNLENBQUMsSUFBSSxDQUFDLEtBQUssQ0FBQyxFQUFFLEdBQUcsQ0FBQyxDQUFDO2dCQUM5RSxNQUFNO1lBQ1YsS0FBSyxJQUFJO2dCQUNMLElBQUksS0FBSyxDQUFDLE9BQU8sQ0FBQyxJQUFJLENBQUMsS0FBSyxDQUFDLElBQUksSUFBSSxDQUFDLEtBQUssQ0FBQyxNQUFNLEVBQUUsQ0FBQztvQkFDakQsQ0FBQyxHQUFH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