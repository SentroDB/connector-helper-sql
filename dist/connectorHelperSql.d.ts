import DBManagerTypes, { SegmentCondition } from "@sentrodb/connector-node-types";
export default class ConnectorHelperSql {
    private dbHandler;
    private dbConfig;
    connect({ config }: {
        config: DBManagerTypes.DBConfig;
    }): Promise<void>;
    disconnect(): Promise<void>;
    getSchemaDetails(): Promise<{
        tables: DBManagerTypes.Table[];
    }>;
    get({ table, where, limit, offset, orderBy, orderDirection, search, searchColumns, columns, extraConditions }: {
        table: string;
        where?: any;
        limit?: number;
        offset?: number;
        orderBy?: string;
        orderDirection?: "asc" | "desc";
        search?: string;
        searchColumns?: string[];
        columns?: string[];
        extraConditions?: SegmentCondition[];
    }): Promise<{}[] | undefined>;
    getSingle({ table, where }: {
        table: string;
        where?: {
            [key: string]: string;
        };
    }): Promise<{
        [x: string]: any;
    } | undefined>;
    update({ table, data, where }: {
        table: string;
        data: DBManagerSchema.UpdateBy<DBManagerSchema.TableName>["patch"];
        where: DBManagerSchema.UpdateBy<DBManagerSchema.TableName>["where"];
    }): Promise<import("kysely").UpdateResult[] | undefined>;
    insert({ table, data }: {
        table: string;
        data: any;
    }): Promise<import("kysely").InsertResult[] | undefined>;
    delete({ table, where, single }: {
        table: string;
        where: any;
        single: boolean;
    }): Promise<import("kysely").DeleteResult | import("kysely").DeleteResult[] | undefined>;
    count({ table, where, search, searchColumns, extraConditions, }: {
        table: string;
        where?: Record<string, unknown>;
        search?: string;
        searchColumns?: string[];
        extraConditions?: SegmentCondition[];
    }): Promise<{
        count: string | number | bigint;
    }[] | undefined>;
    query({ sql: rawSql, params, schema }: {
        sql: string;
        params?: any[];
        schema?: string;
    }): Promise<{
        rows: any[];
        columns: string[];
    }>;
}
//# sourceMappingURL=connectorHelperSql.d.ts.map