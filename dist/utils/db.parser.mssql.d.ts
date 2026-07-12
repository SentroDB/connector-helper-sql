import { Kysely } from "kysely";
import DBManagerTypes from "@sentrodb/connector-node-types";
import { DbParser } from "./db.parser";
/**
 * SQL Server flavour of the schema parser. Reads INFORMATION_SCHEMA plus the
 * sys.* catalog views (FKs and indexes are not reliably exposed through
 * INFORMATION_SCHEMA on MSSQL) and maps the results into the same
 * DBManagerTypes shapes the Postgres parser produces. All shared logic
 * (getSchemaDetails, Prisma implicit M2M folding) is inherited from DbParser.
 */
export declare class MssqlDbParser extends DbParser {
    private schema;
    constructor(dbHandler: Kysely<any>, schema?: string);
    getTables(): Promise<DBManagerTypes.Table[]>;
    getConstraints(): Promise<DBManagerTypes.Constraint[]>;
    getIndexes(): Promise<DBManagerTypes.Index[]>;
    /**
     * MSSQL wraps column defaults in parentheses, e.g. "((0))" or
     * "(getdate())". Strip the outer wrapping so consumers see the bare value.
     */
    private normalizeDefault;
}
//# sourceMappingURL=db.parser.mssql.d.ts.map