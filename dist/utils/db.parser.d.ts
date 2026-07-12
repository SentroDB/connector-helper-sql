import { Kysely } from "kysely";
import DBManagerTypes from "@sentrodb/connector-node-types";
export declare class DbParser {
    protected client: Kysely<any>;
    constructor(dbHandler: Kysely<any>);
    getSchemaDetails(): Promise<{
        tables: DBManagerTypes.Table[];
    }>;
    /**
     * Prisma's implicit many-to-many relations create junction tables named
     * `_ModelAToModelB` (or `_RelationName` for named relations) with exactly
     * two FK columns named `A` and `B`. They aren't user-facing entities, so
     * we drop them and surface the relation as a virtual array-valued column
     * on each side of the relationship. Synthetic constraints are kept (now
     * pointing at the synthetic column) so ERD/relationships consumers still
     * see the M2M edge.
     */
    private foldPrismaImplicitM2M;
    private pickM2MColumnName;
    private pluralizeLowercase;
    private buildM2MSyntheticColumn;
    getTables(): Promise<DBManagerTypes.Table[]>;
    getConstraints(): Promise<DBManagerTypes.Constraint[]>;
    getIndexes(): Promise<DBManagerTypes.Index[]>;
}
//# sourceMappingURL=db.parser.d.ts.map