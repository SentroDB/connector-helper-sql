import { Kysely, sql } from "kysely";
import DBManagerTypes, { ColumnGeneratedType } from "@sentrodb/connector-node-types";


export class DbParser {

    private client: Kysely<any>;

    constructor(dbHandler: Kysely<any>) {
        this.client = dbHandler;
    }

    async getSchemaDetails(): Promise<{
        tables: DBManagerTypes.Table[];
    }> {
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

    async getTables(): Promise<DBManagerTypes.Table[]> {
        const columns = await sql<any>`
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

        const tables: DBManagerTypes.Table[] = [];

        columns.rows.forEach((row) => {
            const {
                table_name,
                name,
                type,
                nullable,
                default: defaultValue,
                primary_key,
                foreign_key,
                unique,
                autoincrement,
                generated_type,
                enum_values: enumValues,
                is_updated_at,
            } = row;

            let enum_values: string[] = [];
            if (enumValues) {
                enum_values = (enumValues as string).slice(1, -1).split(",") || [];
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

            let generatedType: ColumnGeneratedType = generated_type as ColumnGeneratedType;
            let isGenerated: boolean = generated_type !== 'none';
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

    async getConstraints(): Promise<DBManagerTypes.Constraint[]> {
        const tableConstraints = await sql<any>`
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

        const constraints: DBManagerTypes.Constraint[] = [];

        tableConstraints.rows.forEach((row) => {
            const { table_name, name, column, reference_table, reference_column, onUpdate, onDelete, is_unique_fk } =
                row;

            const relationshipType: DBManagerTypes.RelationshipType = is_unique_fk ? "one-to-one" : "one-to-many";

            let constraint = constraints.find((c) => c.name === name);
            if (!constraint) {
                constraint = {
                    table: table_name,
                    name,
                    column,
                    reference:
                        reference_table
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

    async getIndexes(): Promise<DBManagerTypes.Index[]> {
        const indexList = await sql<any>`
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

        const indexes: DBManagerTypes.Index[] = [];

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