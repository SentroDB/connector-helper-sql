import DBManagerTypes from "@sentrodb/connector-node-types";
import { DbParser } from "./db.parser";
/**
 * MySQL flavour of the schema parser. Reads information_schema scoped to the
 * connected database (DATABASE()) and maps the results into the same
 * DBManagerTypes shapes the Postgres parser produces. All shared logic
 * (getSchemaDetails, Prisma implicit M2M folding) is inherited from DbParser.
 */
export declare class MysqlDbParser extends DbParser {
    getTables(): Promise<DBManagerTypes.Table[]>;
    getConstraints(): Promise<DBManagerTypes.Constraint[]>;
    getIndexes(): Promise<DBManagerTypes.Index[]>;
    private parseEnumValues;
}
//# sourceMappingURL=db.parser.mysql.d.ts.map