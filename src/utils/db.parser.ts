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
        const [rawTables, rawConstraints, rawIndexes] = await Promise.all([
            this.getTables(),
            this.getConstraints(),
            this.getIndexes(),
        ]);

        const { tables, constraints, indexes } = this.foldPrismaImplicitM2M(
            rawTables,
            rawConstraints,
            rawIndexes,
        );

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

    /**
     * Prisma's implicit many-to-many relations create junction tables named
     * `_ModelAToModelB` (or `_RelationName` for named relations) with exactly
     * two FK columns named `A` and `B`. They aren't user-facing entities, so
     * we drop them and surface the relation as a virtual array-valued column
     * on each side of the relationship. Synthetic constraints are kept (now
     * pointing at the synthetic column) so ERD/relationships consumers still
     * see the M2M edge.
     */
    private foldPrismaImplicitM2M(
        tables: DBManagerTypes.Table[],
        constraints: DBManagerTypes.Constraint[],
        indexes: DBManagerTypes.Index[],
    ): {
        tables: DBManagerTypes.Table[];
        constraints: DBManagerTypes.Constraint[];
        indexes: DBManagerTypes.Index[];
    } {
        const junctionNames = new Set<string>();
        const syntheticConstraints: DBManagerTypes.Constraint[] = [];

        for (const table of tables) {
            if (!table.name.startsWith("_")) continue;
            if (table.columns.length !== 2) continue;

            const colA = table.columns.find((c) => c.name === "A");
            const colB = table.columns.find((c) => c.name === "B");
            if (!colA?.foreign_key || !colB?.foreign_key) continue;

            const fkA = constraints.find((c) => c.table === table.name && c.column === "A");
            const fkB = constraints.find((c) => c.table === table.name && c.column === "B");
            if (!fkA?.reference || !fkB?.reference) continue;

            junctionNames.add(table.name);

            const relationLabel = table.name.slice(1);
            const sideA = tables.find((t) => t.name === fkA.reference!.table);
            const sideB = tables.find((t) => t.name === fkB.reference!.table);
            if (!sideA || !sideB) continue;

            const aType = sideA.columns.find((c) => c.name === fkA.reference!.column)?.type ?? "text";
            const bType = sideB.columns.find((c) => c.name === fkB.reference!.column)?.type ?? "text";

            const aColName = this.pickM2MColumnName(sideA, fkB.reference.table, relationLabel);
            const bColName = this.pickM2MColumnName(sideB, fkA.reference.table, relationLabel);

            sideA.columns.push(this.buildM2MSyntheticColumn({
                name: aColName,
                elementType: bType,
                references: { table: fkB.reference.table, column: fkB.reference.column },
                junction: { table: table.name, sourceColumn: "A", targetColumn: "B" },
            }));
            sideB.columns.push(this.buildM2MSyntheticColumn({
                name: bColName,
                elementType: aType,
                references: { table: fkA.reference.table, column: fkA.reference.column },
                junction: { table: table.name, sourceColumn: "B", targetColumn: "A" },
            }));

            syntheticConstraints.push({
                table: fkA.reference.table,
                name: `${relationLabel}__a_to_b`,
                column: aColName,
                reference: { table: fkB.reference.table, column: fkB.reference.column },
                onUpdate: fkA.onUpdate,
                onDelete: fkA.onDelete,
                relationshipType: "many-to-many",
                isUnique: false,
                junction: { table: table.name, sourceColumn: "A", targetColumn: "B" },
            });
            syntheticConstraints.push({
                table: fkB.reference.table,
                name: `${relationLabel}__b_to_a`,
                column: bColName,
                reference: { table: fkA.reference.table, column: fkA.reference.column },
                onUpdate: fkB.onUpdate,
                onDelete: fkB.onDelete,
                relationshipType: "many-to-many",
                isUnique: false,
                junction: { table: table.name, sourceColumn: "B", targetColumn: "A" },
            });
        }

        if (junctionNames.size === 0) {
            return { tables, constraints, indexes };
        }

        return {
            tables: tables.filter((t) => !junctionNames.has(t.name)),
            constraints: [
                ...constraints.filter((c) => !junctionNames.has(c.table)),
                ...syntheticConstraints,
            ],
            indexes: indexes.filter((i) => !junctionNames.has(i.table)),
        };
    }

    private pickM2MColumnName(
        ownerTable: DBManagerTypes.Table,
        targetTableName: string,
        relationLabel: string,
    ): string {
        const taken = new Set(ownerTable.columns.map((c) => c.name));
        const base = this.pluralizeLowercase(targetTableName);
        if (!taken.has(base)) return base;
        const withLabel = `${base}_${relationLabel}`;
        if (!taken.has(withLabel)) return withLabel;
        // Last-resort numeric suffix
        let i = 2;
        while (taken.has(`${base}_${i}`)) i++;
        return `${base}_${i}`;
    }

    private pluralizeLowercase(name: string): string {
        const lower = name.charAt(0).toLowerCase() + name.slice(1);
        if (/(s|sh|ch|x|z)$/i.test(lower)) return lower + "es";
        if (/[^aeiou]y$/i.test(lower)) return lower.slice(0, -1) + "ies";
        return lower + "s";
    }

    private buildM2MSyntheticColumn({
        name,
        elementType,
        references,
        junction,
    }: {
        name: string;
        elementType: string;
        references: { table: string; column: string };
        junction: { table: string; sourceColumn: string; targetColumn: string };
    }): DBManagerTypes.Column {
        return {
            name,
            type: `${elementType}[]`,
            nullable: true,
            default: null,
            primary_key: false,
            foreign_key: false,
            unique: false,
            autoincrement: false,
            isGenerated: false,
            generatedType: "none",
            enum_values: [],
            isMany: true,
            references,
            junction,
            customization: {
                description: "",
                rename: "",
                hideView: true,
                hideEdit: false,
                hideCreate: false,
                readOnly: false,
                position: 0,
                displayType: "",
                editType: "",
                displayPrefix: "",
                displaySuffix: "",
            },
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
                EXISTS (
                    SELECT 1
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                      ON tc.constraint_name = kcu.constraint_name
                     AND tc.constraint_schema = kcu.constraint_schema
                    WHERE tc.constraint_type = 'PRIMARY KEY'
                      AND kcu.table_schema = c.table_schema
                      AND kcu.table_name   = c.table_name
                      AND kcu.column_name  = c.column_name
                ) AS primary_key,
                EXISTS (
                    SELECT 1
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                      ON tc.constraint_name = kcu.constraint_name
                     AND tc.constraint_schema = kcu.constraint_schema
                    WHERE tc.constraint_type = 'FOREIGN KEY'
                      AND kcu.table_schema = c.table_schema
                      AND kcu.table_name   = c.table_name
                      AND kcu.column_name  = c.column_name
                ) AS foreign_key,
                EXISTS (
                    SELECT 1
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                      ON tc.constraint_name = kcu.constraint_name
                     AND tc.constraint_schema = kcu.constraint_schema
                    WHERE tc.constraint_type = 'UNIQUE'
                      AND kcu.table_schema = c.table_schema
                      AND kcu.table_name   = c.table_name
                      AND kcu.column_name  = c.column_name
                ) AS "unique",
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