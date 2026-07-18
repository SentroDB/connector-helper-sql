import { CompiledQuery, Kysely, sql } from "kysely";

import MysqlConnector from "./connectors/mysql";
import PostgresConnector from "./connectors/postgre";
import MssqlConnector from "./connectors/mssql";
import { DbParser } from "./utils/db.parser";
import { MysqlDbParser } from "./utils/db.parser.mysql";
import { MssqlDbParser } from "./utils/db.parser.mssql";
import DBManagerTypes, { SegmentCondition } from "@sentrodb/connector-node-types";

export type JunctionWriteSpec = {
    /** Hidden junction table (e.g. "_DocumentToTag") */
    junctionTable: string;
    /** Junction column holding the parent's PK value (e.g. "A") */
    sourceColumn: string;
    /** Junction column holding the related target's PK value (e.g. "B") */
    targetColumn: string;
    /** Column on the parent table whose value goes into junction.sourceColumn (e.g. "id") */
    parentSourceColumn: string;
    /** Target ids to link to the parent for this relation */
    targetIds: any[];
};

type SqlDialect = DBManagerTypes.DBConfig["type"];

/** Cast a column reference to a string type for text search, per dialect. */
function castToText(ref: any, dialect: SqlDialect) {
    if (dialect === "mysql") return sql`CAST(${ref} AS CHAR)`;
    if (dialect === "mssql") return sql`CAST(${ref} AS NVARCHAR(MAX))`;
    return sql`${ref}::text`;
}

/**
 * Case-insensitive LIKE that works across dialects. Postgres and MySQL
 * default the LIKE escape character to backslash; MSSQL has no default
 * escape character, so it needs an explicit ESCAPE clause.
 */
function caseInsensitiveLike(ref: any, pattern: string, dialect: SqlDialect) {
    const target = castToText(ref, dialect);
    if (dialect === "mssql") {
        return sql`LOWER(${target}) LIKE LOWER(${pattern}) ESCAPE '\\'`;
    }
    return sql`LOWER(${target}) LIKE LOWER(${pattern})`;
}

/**
 * Apply a list of segment-style structured conditions to a Kysely query.
 * Conditions are AND-merged. Unknown operators are ignored.
 */
function applySegmentConditions<Q extends { where: any }>(
    query: Q,
    conditions: SegmentCondition[] | undefined,
    dialect: SqlDialect,
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

    private get dialect(): SqlDialect {
        return this.dbConfig?.type ?? "postgres";
    }

    /**
     * OR-merged case-insensitive LIKE clause over the given columns, with
     * LIKE wildcards in the search term escaped.
     */
    private buildSearchClause(search: string, searchColumns: string[]) {
        const escaped = search.replace(/[\\%_]/g, "\\$&");
        const likePattern = `%${escaped}%`;
        return searchColumns
            .map((col) => caseInsensitiveLike(sql.ref(`t.${col}`), likePattern, this.dialect))
            .reduce((acc, piece) => (acc ? sql`${acc} OR ${piece}` : piece), undefined as any);
    }

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
        if (this.dbConfig?.type === "mysql") {
            return (new MysqlDbParser(this.dbHandler)).getSchemaDetails();
        }
        if (this.dbConfig?.type === "mssql") {
            return (new MssqlDbParser(this.dbHandler, this.dbConfig.schema)).getSchemaDetails();
        }
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

        query = applySegmentConditions(query as any, extraConditions, this.dialect);

        if (search && searchColumns?.length) {
            const clause = this.buildSearchClause(search, searchColumns);
            if (clause) {
                query = query.where(clause);
            }
        }

        if (orderBy) {
            query = query.orderBy(sql.ref(`t.${orderBy}`), orderDirection ?? "asc");
        } else if (this.dialect === "mssql" && (typeof limit === "number" || typeof offset === "number")) {
            // MSSQL compiles limit/offset to OFFSET ... FETCH, which is a
            // syntax error without an ORDER BY. Inject the standard no-op.
            query = query.orderBy(sql`(SELECT NULL)`);
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
        { table, data, where, junctions }: {
            table: string,
            data: DBManagerSchema.UpdateBy<DBManagerSchema.TableName>["patch"],
            where: DBManagerSchema.UpdateBy<DBManagerSchema.TableName>["where"],
            junctions?: JunctionWriteSpec[],
        }
    ) {
        if (!this.dbHandler) return;

        if (!where || !Object.keys(where).length) {
            throw new Error(
                `Refusing to update "${table}" without a where clause — this would affect every row.`,
            );
        }

        if (!junctions?.length) {
            let query = this.dbHandler.updateTable(table).set(data);
            query = query.where((eb) => eb.and(where));
            return query.execute();
        }

        return this.dbHandler.transaction().execute(async (trx) => {
            let q = trx.updateTable(table).set(data);
            if (where) q = q.where((eb) => eb.and(where));
            const result = await q.execute();

            const sourceValues = await this.collectSourceValues(trx, table, where, junctions);
            for (const j of junctions) {
                if (!sourceValues.length) continue;
                await trx
                    .deleteFrom(j.junctionTable)
                    .where(j.sourceColumn, "in", sourceValues as any)
                    .execute();
                const rows: Record<string, any>[] = [];
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

    public async insert({ table, data, junctions }: {
        table: string,
        data: any,
        junctions?: JunctionWriteSpec[],
    }) {
        if (!this.dbHandler) return;

        if (!junctions?.length) {
            return this.dbHandler.insertInto(table).values(data).execute();
        }

        return this.dbHandler.transaction().execute(async (trx) => {
            const inserted = await this.insertReturning(trx, table, data);

            for (const j of junctions) {
                const rows: Record<string, any>[] = [];
                for (const parent of inserted) {
                    // Fall back to the MySQL auto-increment id when the PK
                    // wasn't part of the input data (see insertReturning).
                    const sourceValue = (parent as any)[j.parentSourceColumn] ?? (parent as any).insertId;
                    if (sourceValue == null) continue;
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
    private async insertReturning(
        trx: Kysely<any>,
        table: string,
        data: any,
    ): Promise<Record<string, any>[]> {
        if (this.dialect === "mssql") {
            return trx.insertInto(table).values(data).outputAll("inserted").execute() as Promise<Record<string, any>[]>;
        }
        if (this.dialect === "mysql") {
            const result = await trx.insertInto(table).values(data).executeTakeFirst();
            const rows: Record<string, any>[] = Array.isArray(data) ? data : [data];
            const insertId = result?.insertId;
            return rows.map((row, i) =>
                insertId != null ? { insertId: Number(insertId) + i, ...row } : { ...row },
            );
        }
        return trx.insertInto(table).values(data).returningAll().execute() as Promise<Record<string, any>[]>;
    }

    /**
     * Read target ids from a junction table for a given source row. Used by
     * the connector to populate M2M multi-selects with current selections.
     */
    public async getRelatedIds({
        junctionTable,
        sourceColumn,
        sourceValue,
        targetColumn,
    }: {
        junctionTable: string;
        sourceColumn: string;
        sourceValue: any;
        targetColumn: string;
    }): Promise<any[]> {
        if (!this.dbHandler) return [];
        const rows = await this.dbHandler
            .selectFrom(junctionTable)
            .select(targetColumn)
            .where(sourceColumn, "=", sourceValue)
            .execute();
        return rows.map((r: any) => r[targetColumn]);
    }

    private async collectSourceValues(
        trx: Kysely<any>,
        table: string,
        where: DBManagerSchema.UpdateBy<DBManagerSchema.TableName>["where"],
        junctions: JunctionWriteSpec[],
    ): Promise<any[]> {
        const cols = Array.from(new Set(junctions.map((j) => j.parentSourceColumn)));
        if (!cols.length) return [];
        let q = trx.selectFrom(table).select(cols as any);
        if (where) q = q.where((eb: any) => eb.and(where));
        const rows = await q.execute();
        // Single source column case (the common one) — flatten to array of values.
        if (cols.length === 1) {
            return rows.map((r: any) => r[cols[0]]).filter((v) => v != null);
        }
        // Multi-source case shouldn't happen with Prisma implicit M2M (always PK),
        // but be safe: only return if all junctions share the same source column.
        return rows.map((r: any) => r[cols[0]]).filter((v) => v != null);
    }

    public async delete({ table, where, single }: { table: string, where: any, single: boolean }) {
        if (!this.dbHandler) return;

        if (!where || typeof where !== "object" || !Object.keys(where).length) {
            throw new Error(
                `Refusing to delete from "${table}" without a where clause — this would affect every row.`,
            );
        }

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

        q = applySegmentConditions(q as any, extraConditions, this.dialect);

        if (search && searchColumns?.length) {
            const orClause = this.buildSearchClause(search, searchColumns);
            if (orClause) {
                q = q.where(orClause);
            }
        }

        return q.execute();
    }

    public async query({ sql: rawSql, params, schema }: { sql: string, params?: any[], schema?: string }): Promise<{ rows: any[], columns: string[] }> {
        if (!this.dbHandler) throw new Error("Database handler is not initialized.");

        if (schema) {
            if (!/^[A-Za-z_][A-Za-z0-9_$]*$/.test(schema)) {
                throw new Error(`ConnectorHelperSql.query: invalid "schema" argument: ${String(schema)}`);
            }
            if (this.dialect === "postgres") {
                await this.dbHandler.executeQuery(
                    CompiledQuery.raw(`SET search_path TO "${schema}"`)
                );
            } else if (this.dialect === "mysql") {
                // In MySQL a schema is a database
                await this.dbHandler.executeQuery(
                    CompiledQuery.raw(`USE \`${schema}\``)
                );
            } else {
                // MSSQL has no session-level default-schema switch
                throw new Error(`ConnectorHelperSql.query: schema switching is not supported on ${this.dialect}; qualify table names instead.`);
            }
        }

        const compiled = CompiledQuery.raw(rawSql, params ?? []);
        const result = await this.dbHandler.executeQuery(compiled);

        const rows = (result.rows as any[]) ?? [];
        const columns = rows.length > 0 ? Object.keys(rows[0]) : [];

        return { rows, columns };
    }
}