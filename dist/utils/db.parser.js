"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.DbParser = void 0;
const kysely_1 = require("kysely");
class DbParser {
    constructor(dbHandler) {
        this.client = dbHandler;
    }
    async getSchemaDetails() {
        const [rawTables, rawConstraints, rawIndexes] = await Promise.all([
            this.getTables(),
            this.getConstraints(),
            this.getIndexes(),
        ]);
        const { tables, constraints, indexes } = this.foldPrismaImplicitM2M(rawTables, rawConstraints, rawIndexes);
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
    foldPrismaImplicitM2M(tables, constraints, indexes) {
        const junctionNames = new Set();
        const syntheticConstraints = [];
        for (const table of tables) {
            if (!table.name.startsWith("_"))
                continue;
            if (table.columns.length !== 2)
                continue;
            const colA = table.columns.find((c) => c.name === "A");
            const colB = table.columns.find((c) => c.name === "B");
            if (!colA?.foreign_key || !colB?.foreign_key)
                continue;
            const fkA = constraints.find((c) => c.table === table.name && c.column === "A");
            const fkB = constraints.find((c) => c.table === table.name && c.column === "B");
            if (!fkA?.reference || !fkB?.reference)
                continue;
            junctionNames.add(table.name);
            const relationLabel = table.name.slice(1);
            const sideA = tables.find((t) => t.name === fkA.reference.table);
            const sideB = tables.find((t) => t.name === fkB.reference.table);
            if (!sideA || !sideB)
                continue;
            const aType = sideA.columns.find((c) => c.name === fkA.reference.column)?.type ?? "text";
            const bType = sideB.columns.find((c) => c.name === fkB.reference.column)?.type ?? "text";
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
    pickM2MColumnName(ownerTable, targetTableName, relationLabel) {
        const taken = new Set(ownerTable.columns.map((c) => c.name));
        const base = this.pluralizeLowercase(targetTableName);
        if (!taken.has(base))
            return base;
        const withLabel = `${base}_${relationLabel}`;
        if (!taken.has(withLabel))
            return withLabel;
        // Last-resort numeric suffix
        let i = 2;
        while (taken.has(`${base}_${i}`))
            i++;
        return `${base}_${i}`;
    }
    pluralizeLowercase(name) {
        const lower = name.charAt(0).toLowerCase() + name.slice(1);
        if (/(s|sh|ch|x|z)$/i.test(lower))
            return lower + "es";
        if (/[^aeiou]y$/i.test(lower))
            return lower.slice(0, -1) + "ies";
        return lower + "s";
    }
    buildM2MSyntheticColumn({ name, elementType, references, junction, }) {
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
    async getTables() {
        const columns = await (0, kysely_1.sql) `
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
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZGIucGFyc2VyLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vc3JjL3V0aWxzL2RiLnBhcnNlci50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7QUFBQSxtQ0FBcUM7QUFJckMsTUFBYSxRQUFRO0lBSWpCLFlBQVksU0FBc0I7UUFDOUIsSUFBSSxDQUFDLE1BQU0sR0FBRyxTQUFTLENBQUM7SUFDNUIsQ0FBQztJQUVELEtBQUssQ0FBQyxnQkFBZ0I7UUFHbEIsTUFBTSxDQUFDLFNBQVMsRUFBRSxjQUFjLEVBQUUsVUFBVSxDQUFDLEdBQUcsTUFBTSxPQUFPLENBQUMsR0FBRyxDQUFDO1lBQzlELElBQUksQ0FBQyxTQUFTLEVBQUU7WUFDaEIsSUFBSSxDQUFDLGNBQWMsRUFBRTtZQUNyQixJQUFJLENBQUMsVUFBVSxFQUFFO1NBQ3BCLENBQUMsQ0FBQztRQUVILE1BQU0sRUFBRSxNQUFNLEVBQUUsV0FBVyxFQUFFLE9BQU8sRUFBRSxHQUFHLElBQUksQ0FBQyxxQkFBcUIsQ0FDL0QsU0FBUyxFQUNULGNBQWMsRUFDZCxVQUFVLENBQ2IsQ0FBQztRQUVGLFdBQVcsQ0FBQyxPQUFPLENBQUMsQ0FBQyxVQUFVLEVBQUUsRUFBRTtZQUMvQixNQUFNLEtBQUssR0FBRyxNQUFNLENBQUMsSUFBSSxDQUFDLENBQUMsQ0FBQyxFQUFFLEVBQUUsQ0FBQyxDQUFDLENBQUMsSUFBSSxLQUFLLFVBQVUsQ0FBQyxLQUFLLENBQUMsQ0FBQztZQUM5RCxJQUFJLEtBQUssRUFBRSxDQUFDO2dCQUNSLEtBQUssQ0FBQyxXQUFXLENBQUMsSUFBSSxDQUFDLFVBQVUsQ0FBQyxDQUFDO1lBQ3ZDLENBQUM7UUFDTCxDQUFDLENBQUMsQ0FBQztRQUVILE9BQU8sQ0FBQyxPQUFPLENBQUMsQ0FBQyxLQUFLLEVBQUUsRUFBRTtZQUN0QixNQUFNLEtBQUssR0FBRyxNQUFNLENBQUMsSUFBSSxDQUFDLENBQUMsQ0FBQyxFQUFFLEVBQUUsQ0FBQyxDQUFDLENBQUMsSUFB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