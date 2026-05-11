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
    update({ table, data, where, junctions }: {
        table: string;
        data: DBManagerSchema.UpdateBy<DBManagerSchema.TableName>["patch"];
        where: DBManagerSchema.UpdateBy<DBManagerSchema.TableName>["where"];
        junctions?: JunctionWriteSpec[];
    }): Promise<import("kysely").UpdateResult[] | undefined>;
    insert({ table, data, junctions }: {
        table: string;
        data: any;
        junctions?: JunctionWriteSpec[];
    }): Promise<{
        [x: string]: any;
    }[] | import("kysely").InsertResult[] | undefined>;
    /**
     * Read target ids from a junction table for a given source row. Used by
     * the connector to populate M2M multi-selects with current selections.
     */
    getRelatedIds({ junctionTable, sourceColumn, sourceValue, targetColumn, }: {
        junctionTable: string;
        sourceColumn: string;
        sourceValue: any;
        targetColumn: string;
    }): Promise<any[]>;
    private collectSourceValues;
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