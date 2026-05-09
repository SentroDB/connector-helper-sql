import { Kysely } from "kysely";
import DBManagerTypes from "@sentrodb/connector-node-types";
export declare class DbParser {
    private client;
    constructor(dbHandler: Kysely<any>);
    getSchemaDetails(): Promise<{
        tables: DBManagerTypes.Table[];
    }>;
    getTables(): Promise<DBManagerTypes.Table[]>;
    getConstraints(): Promise<DBManagerTypes.Constraint[]>;
    getIndexes(): Promise<DBManagerTypes.Index[]>;
}
//# sourceMappingURL=db.parser.d.ts.map