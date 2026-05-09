import DBManagerTypes from "@sentrodb/connector-node-types";
import { Kysely, PostgresDialect } from "kysely";
import { Pool as PostgresPool } from "pg";

export default function PostgresConnector<T>(config: DBManagerTypes.DBConfig) {
    return new Kysely<T>({
        dialect: new PostgresDialect({
            pool: new PostgresPool(config)
        })
    });
}