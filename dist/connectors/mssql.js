"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
exports.default = MssqlConnector;
const kysely_1 = require("kysely");
const tedious = __importStar(require("tedious"));
const tarn = __importStar(require("tarn"));
function MssqlConnector(config) {
    return new kysely_1.Kysely({
        dialect: new kysely_1.MssqlDialect({
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
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibXNzcWwuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi9zcmMvY29ubmVjdG9ycy9tc3NxbC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7OztBQUtBLGlDQThCQztBQW5DRCxtQ0FBOEM7QUFDOUMsaURBQWtDO0FBQ2xDLDJDQUE0QjtBQUc1QixTQUF3QixjQUFjLENBQUksTUFBK0I7SUFDckUsT0FBTyxJQUFJLGVBQU0sQ0FBQztRQUNkLE9BQU8sRUFBRSxJQUFJLHFCQUFZLENBQUM7WUFDdEIsSUFBSSxFQUFFO2dCQUNGLEdBQUcsSUFBSTtnQkFDUCxPQUFPLEVBQUU7b0JBQ0wsR0FBRyxFQUFFLENBQUM7b0JBQ04sR0FBRyxFQUFFLEVBQUU7aUJBQ1Y7YUFDSjtZQUNELE9BQU8sRUFBRTtnQkFDTCxHQUFHLE9BQU87Z0JBQ1YsaUJBQWlCLEVBQUUsR0FBRyxFQUFFLENBQUMsSUFBSSxPQUFPLENBQUMsVUFBVSxDQUFDO29CQUM1QyxjQUFjLEVBQUU7d0JBQ1osT0FBTyxFQUFFOzRCQUNMLFFBQVEsRUFBRSxNQUFNLENBQUMsUUFBUTs0QkFDekIsUUFBUSxFQUFFLE1BQU0sQ0FBQyxJQUFJO3lCQUN4Qjt3QkFDRCxJQUFJLEVBQUUsU0FBUztxQkFDbEI7b0JBQ0QsT0FBTyxFQUFFO3dCQUNMLFFBQVEsRUFBRSxNQUFNLENBQUMsUUFBUTt3QkFDekIsSUFBSSxFQUFFLE1BQU0sQ0FBQyxJQUFJO3dCQUNqQixzQkFBc0IsRUFBRSxJQUFJO3FCQUMvQjtvQkFDRCxNQUFNLEVBQUUsTUFBTSxDQUFDLElBQUk7aUJBQ3RCLENBQUM7YUFDTDtTQUNKLENBQUM7S0FDTCxDQUFDLENBQUM7QUFDUCxDQUFDIn0=