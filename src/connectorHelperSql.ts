import { CompiledQuery, Kysely, sql } from "kysely";

import MysqlConnector from "./connectors/mysql";
import PostgresConnector from "./connectors/postgre";
import MssqlConnector from "./connectors/mssql";
import { DbParser } from "./utils/db.parser";
import DBManagerTypes, { SegmentCondition } from "@sentrodb/connector-node-types";

/**
 * Apply a list of segment-style structured conditions to a Kysely query.
 * Conditions are AND-merged. Unknown operators are ignored.
 */
function applySegmentConditions<Q extends { where: any }>(
    query: Q,
    conditions: SegmentCondition[] | undefined,
    alias = "t",
): Q {
    if (!conditions?.length) return query;
    let q: any = query;
    for (const cond of conditions) {
        if (!cond?.column || !cond.operator) continue;
        const ref = sql.ref(`${alias}.${cond.column}`);
        switch (cond.operator) {
            case "eq":
                q = q.where(ref, "=", cond.value as any);
                break;
            case "neq":
                q = q.where(ref, "!=", cond.value as any);
                break;
            case "gt":
                q = q.where(ref, ">", cond.value as any);
                break;
            case "gte":
                q = q.where(ref, ">=", cond.value as any);
                break;
            case "lt":
                q = q.where(ref, "<", cond.value as any);
                break;
            case "lte":
                q = q.where(ref, "<=", cond.value as any);
                break;
            case "contains":
                q = q.where(sql`LOWER(${ref}::text) LIKE LOWER(${`%${String(cond.value)}%`})`);
                break;
            case "startsWith":
                q = q.where(sql`LOWER(${ref}::text) LIKE LOWER(${`${String(cond.value)}%`})`);
                break;
            case "endsWith":
                q = q.where(sql`LOWER(${ref}::text) LIKE LOWER(${`%${String(cond.value)}`})`);
                break;
            case "in":
                if (Array.isArray(cond.value) && cond.value.length) {
                    q = q.where(ref, "in", cond.value as any);
                }
                break;
            case "notIn":
                if (Array.isArray(cond.value) && cond.value.length) {
                    q = q.where(ref, "not in", cond.value as any);
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

export default class ConnectorHelperSql {
    private dbHandler: Kysely<any> | undefined;
    private dbConfig: DBManagerTypes.DBConfig | undefined;

    public async connect({ config }: { config: DBManagerTypes.DBConfig }) {
        this.dbConfig = config;
        if (config.type == "postgres") {
            this.dbHandler = PostgresConnector(config)
        } else if (config.type == "mysql") {
            this.dbHandler = MysqlConnector(config)
        } else if (config.type == "mssql") {
            this.dbHandler = MssqlConnector(config)
        }
    }

    public async disconnect() {
        if (this.dbHandler) {
            await this.dbHandler.destroy();
            this.dbHandler = undefined;
        }
    }

    public getSchemaDetails() {
        if (!this.dbHandler) throw new Error("Database handler is not initialized.");
        return (new DbParser(this.dbHandler)).getSchemaDetails();
    }

    public async get({ table, where, limit, offset, orderBy, orderDirection, search, searchColumns, columns, extraConditions }: { table: string, where?: any, limit?: number, offset?: number, orderBy?: string, orderDirection?: "asc" | "desc", search?: string, searchColumns?: string[], columns?: string[], extraConditions?: SegmentCondition[] }) {
        if (!this.dbHandler) return;

        if (!table || typeof table !== "string") {
            throw new Error(`ConnectorHelperSql.get: invalid "table" argument: ${String(table)}`);
        }

        const t = sql.table(table).as("t")
        let query = this.dbHandler.selectFrom(t)

        if (columns?.length)
            query = query.select(columns.map((col) => sql.ref(`t.${col}`)) as any);
        else
            query = query.selectAll("t");

        if (where && Object.keys(where).length) {
            for (const [col, val] of Object.entries(where)) {
                query = query.where(sql.ref(`t.${col}`), "=", val as any);
            }
        }

        query = applySegmentConditions(query as any, extraConditions);

        if (search && searchColumns?.length) {
            const escaped = search.replace(/[%_]/g, "\\$&");
            const likePattern = `%${escaped}%`;
            const clause = searchColumns
                .map((col) =>
                    sql`LOWER(${sql.ref(`t.${col}`)}::text) LIKE LOWER(${likePattern})`

                )
                .reduce((acc, piece) => (acc ? sql`${acc} OR ${piece}` : piece), undefined as any);

            if (clause) {
                query = query.where(clause);
            }
        }

        if (orderBy) {
            query = query.orderBy(sql.ref(`t.${orderBy}`), orderDirection ?? "asc");
        }

        if (typeof limit === "number") query = query.limit(limit);
        if (typeof offset === "number") query = query.offset(offset);

        return query.execute();
    }

    public async getSingle({ table, where }: { table: string, where?: { [key: string]: string } }) {
        if (!this.dbHandler) return;

        if (!table || typeof table !== "string") {
            throw new Error(`ConnectorHelperSql.getSingle: invalid "table" argument: ${String(table)}`);
        }

        const t = sql.table(table).as("t")
        let query = this.dbHandler.selectFrom(t).selectAll("t");

        if (where && Object.keys(where).length) {
            for (const [col, val] of Object.entries(where)) {
                query = query.where(sql.ref(`t.${col}`), "=", val);
            }
        }

        return query.executeTakeFirst();
    }

    public async update(
        { table, data, where }: {
            table: string,
            data: DBManagerSchema.UpdateBy<DBManagerSchema.TableName>["patch"],
            where: DBManagerSchema.UpdateBy<DBManagerSchema.TableName>["where"]
        }
    ) {
        if (!this.dbHandler) return;
        const query = this.dbHandler.updateTable(table).set(data);
        if (where) query.where((eb) => eb.and(where));
        return query.execute();
    }

    public async insert({ table, data }: { table: string, data: any }) {
        if (!this.dbHandler) return;
        const query = this.dbHandler.insertInto(table).values(data);
        return query.execute();
    }

    public async delete({ table, where, single }: { table: string, where: any, single: boolean }) {
        if (!this.dbHandler) return;

        let query = this.dbHandler.deleteFrom(table);

        if(single) {
            query = query.where(where)
            return query.executeTakeFirst();
        }
        
        query = query.where(
            (eb) => eb.and(Object.entries(where).map(([col, val]) => {
                return eb.or(
                    (val as any[]).map((v) => eb(col, "=", v as any))
                );
            }))
        );
        
        return query.execute();
    }

    public async count({
        table,
        where,
        search,
        searchColumns,
        extraConditions,
    }: {
        table: string;
        where?: Record<string, unknown>;
        search?: string;
        searchColumns?: string[];
        extraConditions?: SegmentCondition[];
    }) {
        if (!this.dbHandler) return;

        if (!table || typeof table !== "string") {
            throw new Error(`ConnectorHelperSql.count: invalid "table" argument: ${String(table)}`);
        }

        const t = sql.table(table).as("t");

        let q = this.dbHandler
            .selectFrom(t)
            .select(({ fn }) => fn.countAll().as("count"));

        if (where && Object.keys(where).length) {
            for (const [col, val] of Object.entries(where)) {
                q = q.where(sql.ref(`t.${col}`), "=", val as any);
            }
        }

        q = applySegmentConditions(q as any, extraConditions);

        if (search && searchColumns?.length) {
            const escaped = search.replace(/[%_]/g, "\\$&");
            const likePattern = `%${escaped}%`;

            const orClause = searchColumns
                .map((col) =>
                    sql`LOWER(${sql.ref(`t.${col}`)}::text) LIKE LOWER(${likePattern})`
                )
                .reduce((acc, piece) => (acc ? sql`${acc} OR ${piece}` : piece), undefined as any);

            if (orClause) {
                q = q.where(orClause);
            }
        }

        return q.execute();
    }

    public async query({ sql: rawSql, params, schema }: { sql: string, params?: any[], schema?: string }): Promise<{ rows: any[], columns: string[] }> {
        if (!this.dbHandler) throw new Error("Database handler is not initialized.");

        if (schema) {
            await this.dbHandler.executeQuery(
                CompiledQuery.raw(`SET search_path TO "${schema}"`)
            );
        }

        const compiled = CompiledQuery.raw(rawSql, params ?? []);
        const result = await this.dbHandler.executeQuery(compiled);

        const rows = (result.rows as any[]) ?? [];
        const columns = rows.length > 0 ? Object.keys(rows[0]) : [];

        return { rows, columns };
    }
}