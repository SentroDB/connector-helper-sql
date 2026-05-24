"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.default = MysqlConnector;
const kysely_1 = require("kysely");
const mysql2_1 = require("mysql2");
function MysqlConnector(config) {
    return new kysely_1.Kysely({
        dialect: new kysely_1.MysqlDialect({
            pool: (0, mysql2_1.createPool)({
                ...config,
                ssl: config.ssl ? { rejectUnauthorized: false } : undefined,
            })
        })
    });
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibXlzcWwuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi9zcmMvY29ubmVjdG9ycy9teXNxbC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOztBQUlBLGlDQVNDO0FBWkQsbUNBQThDO0FBQzlDLG1DQUFvQztBQUVwQyxTQUF3QixjQUFjLENBQUksTUFBK0I7SUFDckUsT0FBTyxJQUFJLGVBQU0sQ0FBSTtRQUNqQixPQUFPLEVBQUUsSUFBSSxxQkFBWSxDQUFDO1lBQ3RCLElBQUksRUFBRSxJQUFBLG1CQUFVLEVBQUM7Z0JBQ2IsR0FBRyxNQUFNO2dCQUNULEdBQUcsRUFBRSxNQUFNLENBQUMsR0FBRyxDQUFDLENBQUMsQ0FBQyxFQUFFLGtCQUFrQixFQUFFLEtBQUssRUFBRSxDQUFDLENBQUMsQ0FBQyxTQUFTO2FBQzlELENBQUM7U0FDTCxDQUFDO0tBQ0wsQ0FBQyxDQUFDO0FBQ1AsQ0FBQyJ9