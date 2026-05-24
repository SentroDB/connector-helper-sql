"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.default = PostgresConnector;
const kysely_1 = require("kysely");
const pg_1 = require("pg");
function PostgresConnector(config) {
    return new kysely_1.Kysely({
        dialect: new kysely_1.PostgresDialect({
            pool: new pg_1.Pool({
                ...config,
                ssl: config.ssl ? { rejectUnauthorized: false } : false,
            }),
        })
    });
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicG9zdGdyZS5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uL3NyYy9jb25uZWN0b3JzL3Bvc3RncmUudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7QUFJQSxvQ0FTQztBQVpELG1DQUFpRDtBQUNqRCwyQkFBMEM7QUFFMUMsU0FBd0IsaUJBQWlCLENBQUksTUFBK0I7SUFDeEUsT0FBTyxJQUFJLGVBQU0sQ0FBSTtRQUNqQixPQUFPLEVBQUUsSUFBSSx3QkFBZSxDQUFDO1lBQ3pCLElBQUksRUFBRSxJQUFJLFNBQVksQ0FBQztnQkFDbkIsR0FBRyxNQUFNO2dCQUNULEdBQUcsRUFBRSxNQUFNLENBQUMsR0FBRyxDQUFDLENBQUMsQ0FBQyxFQUFFLGtCQUFrQixFQUFFLEtBQUssRUFBRSxDQUFDLENBQUMsQ0FBQyxLQUFLO2FBQzFELENBQUM7U0FDTCxDQUFDO0tBQ0wsQ0FBQyxDQUFDO0FBQ1AsQ0FBQyJ9