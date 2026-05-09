import DBManagerTypes from "@sentrodb/connector-node-types";
import { Kysely, MysqlDialect } from "kysely";
import { createPool } from "mysql2";

export default function MysqlConnector<T>(config: DBManagerTypes.DBConfig) {
    return new Kysely<T>({
        dialect: new MysqlDialect({
            pool: createPool(config)
        })
    });
}