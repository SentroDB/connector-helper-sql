"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.DbParser = void 0;
const kysely_1 = require("kysely");
class DbParser {
    constructor(dbHandler) {
        this.client = dbHandler;
    }
    async getSchemaDetails() {
        const [tables, constraints, indexes] = await Promise.all([
            this.getTables(),
            this.getConstraints(),
            this.getIndexes(),
        ]);
        constraints.forEach((constraint) => {
            const table = tables.find((t) => t.name === constraint.table);
            if (table) {
                table.constraints.push(constraint);
            }
        });
        indexes.forEach((index) => {
            const table = tables.find((t) => t.name === index.table);
            if (table) {
                table.indexes.push(index);
            }
        });
        return {
            tables,
        };
    }
    async getTables() {
        const columns = await (0, kysely_1.sql) `
           SELECT 
            c.table_name,
            c.column_name AS name,
            c.data_type   AS type,
            c.udt_name,
            c.is_nullable = 'YES' AS nullable,
            c.column_default AS "default",
            CASE WHEN tc.constraint_type = 'PRIMARY KEY' THEN True ELSE False END AS primary_key,
            CASE 
                WHEN EXISTS (
                    SELECT 1 
                    FROM information_schema.table_constraints tc2
                    JOIN information_schema.key_column_usage kcu2 
                    ON tc2.constraint_name = kcu2.constraint_name
                    WHERE tc2.constraint_type = 'FOREIGN KEY' 
                    AND kcu2.table_name = c.table_name
                    AND kcu2.column_name = c.column_name
                ) THEN True ELSE False END AS foreign_key,
            CASE WHEN tc.constraint_type = 'UNIQUE' THEN True ELSE False END AS "unique",
            'N/A' AS "check",
            CASE 
                WHEN c.column_default LIKE 'nextval%' THEN True 
                ELSE False 
            END AS autoincrement,
            CASE 
                WHEN c.column_default LIKE 'nextval%' THEN 'sequence'
                WHEN c.column_default LIKE '%cuid%'   THEN 'cuid'
                WHEN c.column_default LIKE '%uuid%'   THEN 'uuid'
                ELSE 'none'
            END AS generated_type,
            enum_info.enum_values,
            CASE
                WHEN c.column_name IN ('updatedAt', 'updated_at', 'createdAt', 'created_at')
                AND c.data_type LIKE 'timestamp%'
                THEN TRUE
                ELSE FALSE
            END AS is_updated_at
        FROM information_schema.columns c
        LEFT JOIN information_schema.key_column_usage kcu 
        ON c.column_name = kcu.column_name 
        AND c.table_name  = kcu.table_name
        LEFT JOIN information_schema.table_constraints tc 
        ON kcu.constraint_name = tc.constraint_name
        LEFT JOIN LATERAL (
        SELECT array_agg(e.enumlabel ORDER BY e.enumsortorder) AS enum_values
        FROM pg_type t
        JOIN pg_namespace n ON n.oid = t.typnamespace
        JOIN pg_enum e ON e.enumtypid = t.oid
        WHERE n.nspname = c.table_schema
            AND t.typtype = 'e'
            AND t.typname = c.udt_name
        ) AS enum_info ON TRUE
        WHERE c.table_schema = 'public'
        ORDER BY c.table_name, c.ordinal_position;

        `.execute(this.client);
        const tables = [];
        columns.rows.forEach((row) => {
            const { table_name, name, type, nullable, default: defaultValue, primary_key, foreign_key, unique, autoincrement, generated_type, enum_values: enumValues, is_updated_at, } = row;
            let enum_values = [];
            if (enumValues) {
                enum_values = enumValues.slice(1, -1).split(",") || [];
            }
            let table = tables.find((c) => c.name === table_name);
            if (!table) {
                table = {
                    name: table_name,
                    columns: [],
                    constraints: [],
                    indexes: [],
                    customization: {
                        rename: "",
                        isVisible: true,
                        allowExport: true,
                        allowCreate: true,
                        allowEdit: true,
                        allowDelete: true,
                    }
                };
                tables.push(table);
            }
            let generatedType = generated_type;
            let isGenerated = generated_type !== 'none';
            if (primary_key && type === "text" && !defaultValue) {
                isGenerated = true;
                generatedType = "cuid";
            }
            table.columns.push({
                name,
                type,
                nullable,
                default: defaultValue || (is_updated_at ? 'CURRENT_TIMESTAMP' : null),
                primary_key,
                foreign_key,
                unique: unique || primary_key,
                autoincrement: (primary_key && defaultValue) || defaultValue?.includes('nextval') || autoincrement,
                isGenerated,
                generatedType,
                enum_values,
                customization: {
                    description: "",
                    rename: "",
                    hideView: false,
                    hideEdit: false,
                    hideCreate: false,
                    readOnly: false,
                    position: 0,
                    displayType: "",
                    editType: "",
                    displayPrefix: "",
                    displaySuffix: "",
                }
            });
        });
        return tables;
    }
    async getConstraints() {
        const tableConstraints = await (0, kysely_1.sql) `
            SELECT
                tc.table_name,
                tc.constraint_name AS name,
                kcu.column_name AS column,
                ccu.table_name AS reference_table,
                ccu.column_name AS reference_column,
                rc.update_rule AS onUpdate,
                rc.delete_rule AS onDelete,
                CASE
                    WHEN EXISTS (
                        SELECT 1
                        FROM information_schema.table_constraints tcu
                        JOIN information_schema.key_column_usage kcu2
                            ON tcu.constraint_name = kcu2.constraint_name
                        AND tcu.constraint_schema = kcu2.constraint_schema
                        WHERE tcu.constraint_schema = tc.constraint_schema
                            AND tcu.table_name = tc.table_name
                            AND kcu2.column_name = kcu.column_name
                            AND tcu.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
                    ) THEN TRUE
                    ELSE FALSE
                END AS is_unique_fk
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.referential_constraints AS rc
            ON rc.constraint_name  = tc.constraint_name
            AND rc.constraint_schema = tc.constraint_schema
            LEFT JOIN information_schema.key_column_usage AS kcu
            ON kcu.constraint_name  = tc.constraint_name
            AND kcu.constraint_schema = tc.constraint_schema
            LEFT JOIN information_schema.constraint_column_usage AS ccu
            ON ccu.constraint_name  = tc.constraint_name
            AND ccu.constraint_schema = tc.constraint_schema
            WHERE
                tc.constraint_schema = 'public'
            AND tc.constraint_type = 'FOREIGN KEY'
            ORDER BY
                tc.table_name, tc.constraint_name;
        `.execute(this.client);
        const constraints = [];
        tableConstraints.rows.forEach((row) => {
            const { table_name, name, column, reference_table, reference_column, onUpdate, onDelete, is_unique_fk } = row;
            const relationshipType = is_unique_fk ? "one-to-one" : "one-to-many";
            let constraint = constraints.find((c) => c.name === name);
            if (!constraint) {
                constraint = {
                    table: table_name,
                    name,
                    column,
                    reference: reference_table
                        ? { table: reference_table, column: reference_column }
                        : null,
                    onUpdate,
                    onDelete,
                    relationshipType,
                    isUnique: is_unique_fk,
                };
                constraints.push(constraint);
            }
        });
        return constraints;
    }
    async getIndexes() {
        const indexList = await (0, kysely_1.sql) `
            SELECT 
                i.relname AS index_name,
                t.relname AS table_name,
                a.attname AS column_name,
                CASE 
                    WHEN c.contype = 'p' THEN true
                    ELSE false
                END AS is_primary,
                ix.indisunique AS is_unique
            FROM 
                pg_index AS ix
            JOIN 
                pg_class AS i ON i.oid = ix.indexrelid
            JOIN 
                pg_class AS t ON t.oid = ix.indrelid
            JOIN 
                pg_attribute AS a 
                    ON a.attnum = ANY(ix.indkey) 
                    AND a.attrelid = t.oid
            LEFT JOIN 
                pg_constraint AS c 
                    ON c.conindid = ix.indexrelid 
                    AND c.contype IN ('p', 'u')
            WHERE 
                t.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')
            ORDER BY 
                t.relname, i.relname;
        `.execute(this.client);
        const indexes = [];
        indexList.rows.forEach((row) => {
            const { table_name, index_name, column_name, is_primary, is_unique } = row;
            let index = indexes.find((i) => i.name === index_name);
            if (!index) {
                index = {
                    table: table_name,
                    name: index_name,
                    columns: [],
                    is_primary,
                    is_unique,
                };
                indexes.push(index);
            }
            index.columns.push(column_name);
        });
        return indexes;
    }
}
exports.DbParser = DbParser;
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZGIucGFyc2VyLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vc3JjL3V0aWxzL2RiLnBhcnNlci50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7QUFBQSxtQ0FBcUM7QUFJckMsTUFBYSxRQUFRO0lBSWpCLFlBQVksU0FBc0I7UUFDOUIsSUFBSSxDQUFDLE1BQU0sR0FBRyxTQUFTLENBQUM7SUFDNUIsQ0FBQztJQUVELEtBQUssQ0FBQyxnQkFBZ0I7UUFHbEIsTUFBTSxDQUFDLE1BQU0sRUFBRSxXQUFXLEVBQUUsT0FBTyxDQUFDLEdBQUcsTUFBTSxPQUFPLENBQUMsR0FBRyxDQUFDO1lBQ3JELElBQUksQ0FBQyxTQUFTLEVBQUU7WUFDaEIsSUFBSSxDQUFDLGNBQWMsRUFBRTtZQUNyQixJQUFJLENBQUMsVUFBVSxFQUFFO1NBQ3BCLENBQUMsQ0FBQztRQUVILFdBQVcsQ0FBQyxPQUFPLENBQUMsQ0FBQyxVQUFVLEVBQUUsRUFBRTtZQUMvQixNQUFNLEtBQUssR0FBRyxNQUFNLENBQUMsSUFBSSxDQUFDLENBQUMsQ0FBQyxFQUFFLEVBQUUsQ0FBQyxDQUFDLENBQUMsSUFBSSxLQUFLLFVBQVUsQ0FBQyxLQUFLLENBQUMsQ0FBQztZQUM5RCxJQUFJLEtBQUssRUFBRSxDQUFDO2dCQUNSLEtBQUssQ0FBQyxXQUFXLENBQUMsSUFBSSxDQUFDLFVBQVUsQ0FBQyxDQUFDO1lBQ3ZDLENBQUM7UUFDTCxDQUFDLENBQUMsQ0FBQztRQUVILE9BQU8sQ0FBQyxPQUFPLENBQUMsQ0FBQyxLQUFLLEVBQUUsRUFBRTtZQUN0QixNQUFNLEtBQUssR0FBRyxNQUFNLENBQUMsSUFBSSxDQUFDLENBQUMsQ0FBQyxFQUFFLEVBQUUsQ0FBQyxDQUFDLENBQUMsSUFBSSxLQUFLLEtBQUssQ0FBQyxLQUFLLENBQUMsQ0FBQztZQUN6RCxJQUFJLEtBQUssRUFBRSxDQUFDO2dCQUNSLEtBQUssQ0FBQyxPQUFPLENBQUMsSUFBSSxDQUFDLEtBQUssQ0FBQyxDQUFD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