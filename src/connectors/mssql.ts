import { Kysely, MssqlDialect } from "kysely";
import * as tedious from 'tedious'
import * as tarn from 'tarn'
import DBManagerTypes from "@sentrodb/connector-node-types";

export default function MssqlConnector<T>(config: DBManagerTypes.DBConfig) {
    return new Kysely({
        dialect: new MssqlDialect({
            tarn: {
                ...tarn,
                options: {
                    min: 0,
                    max: 10,
                },
            },
            tedious: {
                ...tedious,
                connectionFactory: () => new tedious.Connection({
                    authentication: {
                        options: {
                            password: config.password,
                            userName: config.user,
                        },
                        type: 'default',
                    },
                    options: {
                        database: config.database,
                        port: config.port,
                        trustServerCertificate: true,
                    },
                    server: config.host,
                }),
            },
        })
    });
}