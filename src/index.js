const parseQuery = require('./queryParser');
const readCSV = require('./csvReader');

function performInnerJoin(data, joinData, joinCondition, fields, table) {
    data = data.flatMap(mainRow => {
        return joinData
            .filter(joinRow => {
                const mainValue = mainRow[joinCondition.left.split('.')[1]];
                const joinValue = joinRow[joinCondition.right.split('.')[1]];
                return mainValue === joinValue;
            })
            .map(joinRow => {
                return fields.reduce((acc, field) => {
                    const [tableName, fieldName] = field.split('.');
                    acc[field] = tableName === table ? mainRow[fieldName] : joinRow[fieldName];
                    return acc;
                }, {});
            });
    });
    return data;
}

function performLeftJoin(data, joinData, joinCondition, fields, table) {
    data = data.flatMap(mainRow => {
        const matchingJoinRows = joinData.filter(joinRow => {
            const mainValue = mainRow[joinCondition.left.split('.')[1]];
            const joinValue = joinRow[joinCondition.right.split('.')[1]];
            return mainValue === joinValue;
        });

        if (matchingJoinRows.length === 0) {
            return [fields.reduce((acc, field) => {
                const [tableName, fieldName] = field.split('.');
                acc[field] = tableName === table ? mainRow[fieldName] : null;
                return acc;
            }, {})];
        }

        return matchingJoinRows.map(joinRow => {
            return fields.reduce((acc, field) => {
                const [tableName, fieldName] = field.split('.');
                acc[field] = tableName === table ? mainRow[fieldName] : joinRow[fieldName];
                return acc;
            }, {});
        });
    });
    return data;
}

function performRightJoin(data, joinData, joinCondition, fields, table) {
    data = data.flatMap(mainRow => {
        const matchingJoinRows = joinData.filter(joinRow => {
            const mainValue = mainRow[joinCondition.left.split('.')[1]];
            const joinValue = joinRow[joinCondition.right.split('.')[1]];
            return mainValue === joinValue;
        });
        if (matchingJoinRows.length === 0) {
            return matchingJoinRows.map(joinRow => {
                return fields.reduce((acc, field) => {
                    const [tableName, fieldName] = field.split('.');
                    acc[field] = tableName === table ? mainRow[fieldName] : joinRow[fieldName];
                    return acc;
                }, {});
            });
        }
        return [fields.reduce((acc, field) => {
            const [tableName, fieldName] = field.split('.');
            acc[field] = tableName === table ? mainRow[fieldName] : null;
            return acc;
        }, {})];
    });
    return data;
}

function evaluateCondition(row, clause) {
    const { field, operator, value } = clause;
    switch (operator) {
        case '=': return row[field] === value;
        case '!=': return row[field] !== value;
        case '>': return row[field] > value;
        case '<': return row[field] < value;
        case '>=': return row[field] >= value;
        case '<=': return row[field] <= value;
        default: throw new Error(`Unsupported operator: ${operator}`);
    }
}

// Helper function to apply GROUP BY and aggregate functions
function applyGroupBy(data, groupByFields, aggregateFunctions) {
    const groupResults = {};

    data.forEach(row => {
        // Generate a key for the group
        const groupKey = groupByFields.map(field => row[field]).join('-');

        // Initialize group in results if it doesn't exist
        if (!groupResults[groupKey]) {
            groupResults[groupKey] = { count: 0, sums: {}, mins: {}, maxes: {} };
            groupByFields.forEach(field => groupResults[groupKey][field] = row[field]);
        }

        // Aggregate calculations
        groupResults[groupKey].count += 1;
        aggregateFunctions.forEach(func => {
            const match = /(\w+)\((\w+)\)/.exec(func);
            if (match) {
                const [, aggFunc, aggField] = match;
                switch (aggFunc.toUpperCase()) {
                    case 'COUNT':
                        groupResults[groupKey].count = (groupResults[groupKey].count || 0) + 1;
                        break;
                    case 'SUM':
                        groupResults[groupKey].sums[aggField] = (groupResults[groupKey].sums[aggField] || 0) + row[aggField];
                        break;
                    case 'MIN':
                        groupResults[groupKey].mins[aggField] = Math.min(groupResults[groupKey].mins[aggField] || Infinity, row[aggField]);
                        break;
                    case 'MAX':
                        groupResults[groupKey].maxes[aggField] = Math.max(groupResults[groupKey].maxes[aggField] || -Infinity, row[aggField]);
                        break;
                }
            }
        });
    });
}
function calculateAggregate(filteredData, fields) {
    const result = {};

    fields.forEach(field => {
        const match = /(\w+)\((\*|\w+)\)/.exec(field);
        if (match) {
            const [, aggFunc, aggField] = match;
            switch (aggFunc.toUpperCase()) {
                case 'COUNT':
                    result[field] = filteredData.length;
                    break;
                case 'SUM':
                    result[field] = filteredData.reduce((acc, row) => acc + parseFloat(row[aggField]), 0);
                    break;
                case 'AVG':
                    result[field] = filteredData.reduce((acc, row) => acc + parseFloat(row[aggField]), 0) / filteredData.length;
                    break;
                case 'MIN':
                    result[field] = Math.min(...filteredData.map(row => parseFloat(row[aggField])));
                    break;
                case 'MAX':
                    result[field] = Math.max(...filteredData.map(row => parseFloat(row[aggField])));
                    break;
                // Additional aggregate functions can be handled here
            }
        }
    });

    return result;
}

async function executeSELECTQuery(query) {
    
    const { fields, table, whereClauses, joinType, joinTable, joinCondition, groupByFields, hasAggregateWithoutGroupBy } = parseQuery(query);
    let data = await readCSV(`${table}.csv`);

    // Perform INNER JOIN if specified
    if (joinTable && joinCondition) {
        const joinData = await readCSV(`${joinTable}.csv`);
        switch (joinType.toUpperCase()) {
            case 'INNER':
                data = performInnerJoin(data, joinData, joinCondition, fields, table);
                break;
            case 'LEFT':
                data = performLeftJoin(data, joinData, joinCondition, fields, table);
                break;
            case 'RIGHT':
                data = performRightJoin(data, joinData, joinCondition, fields, table);
                break;
            // Handle default case or unsupported JOIN types
        }
    }


    // Apply WHERE clause filtering
    const filteredData = whereClauses.length > 0
        ? data.filter(row => whereClauses.every(clause => evaluateCondition(row, clause)))
        : data;

    let result;
    if (hasAggregateWithoutGroupBy) {
        // Special handling for queries like 'SELECT COUNT(*) FROM table'
        result = calculateAggregate(filteredData, fields);

        fields.forEach(field => {
            // Assuming 'field' is just the column name without table prefix
            result[field] = calculateAggregate(data, filteredData, fields);
        });
        } else if (groupByFields) {
            result = applyGroupBy(filteredData, groupByFields, fields);
        } else {
            // Select the specified fields
            result = filteredData.map(row => {
                const selectedRow = {};
                fields.forEach(field => {
                    // Assuming 'field' is just the column name without table prefix
                    selectedRow[field] = row[field];
                });
                return selectedRow;
            });

        }

        return result;
    }

module.exports = executeSELECTQuery;