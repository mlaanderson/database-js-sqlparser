var { parse } = require('node-sqlparser');

const ExtendedSyntax = {
    "DROP": {
        re: /^\s*DROP\s+TABLE\s+(\S+)\s*;?$/i,
        convert: function(sql) {
            if (this.re.test(sql)) {
                var match = this.re.exec(sql);
                return {
                    type: "drop_table",
                    table: match[1]
                }
            }
            return false;
        }
    }
}

class AbstractDriver {

    /**
     * Performs a where operation on a given row to determine if it is a match
     * @param {any} where The where clause
     * @param {any} row The row of data as an JSON like object
     * @param {string|false} namespace If not false, the namespace or table name to prepend to each column name
     */
    doWhere(where, row, namespace = false) {
        if (where === null) return true;

        var getVal = (obj) => { 
            if (obj.type === "column_ref") {
                let field = namespace ? obj.table + "." + obj.column : obj.column
                return row[field];
            }
            if (obj.type === "binary_expr") return this.doWhere(obj, row);
            return obj.value;
        }

        var replaceIfNotPrecededBy = (notPrecededBy, replacement) => {
            return function(match) {
                return match.slice(0, notPrecededBy.length) === notPrecededBy
                ? match
                : replacement;
            }
        }

        var like2RegExp = (like) => {
            var restring = like;
            restring = restring.replace(/([\.\*\?\$\^])/g, "\\$1");
            restring = restring.replace(/(?:\\)?%/g, replaceIfNotPrecededBy('\\', '.*?'));
            restring = restring.replace(/(?:\\)?_/g, replaceIfNotPrecededBy('\\', '.'));
            restring = restring.replace('\\%', '%');
            restring = restring.replace('\\_', '_');
            return new RegExp('^' + restring + '$');
        }

        switch (where.type) {
            case "binary_expr":
                switch(where.operator) {
                    case "=":
                        return getVal(where.left) == getVal(where.right);
                    case "!=":
                    case "<>":
                        return getVal(where.left) != getVal(where.right);
                    case "<":
                        return getVal(where.left) < getVal(where.right);
                    case "<=":
                        return getVal(where.left) <= getVal(where.right);
                    case ">":
                        return getVal(where.left) > getVal(where.right);
                    case ">=":
                        return getVal(where.left) >= getVal(where.right);
                    case "AND":
                        return getVal(where.left) && getVal(where.right);
                    case "OR":
                        return getVal(where.left) && getVal(where.right);
                    case "IS":
                        return getVal(where.left) === getVal(where.right)
                    case "LIKE":
                        return like2RegExp(getVal(where.right)).test(getVal(where.left)) === true;
                    case "NOT LIKE":
                        return like2RegExp(getVal(where.right)).test(getVal(where.left)) === false;
                    default:
                        return false;
                }
                break;
            default:
                return false;
        }
    }

    /**
     * Used to push a row into the data object. If the fields are limited
     * in the query, only places the requested fields.
     * 
     * @param {object} sqlobj 
     * @param {Array} data 
     * @param {object} row 
     */
    chooseFields(sqlobj, data, row, namespace = false) {
        if (sqlobj.columns === "*") {
            data.push(row);
            return;
        }

        let isAggregate = sqlobj.columns.some((col) => { return col.expr.type === 'aggr_func'; });

        if (isAggregate === true) {
            var groupby = () => {
                if (sqlobj.groupby == null) {
                    if (data.length < 1) {
                        data.push({});
                    }
                    return 0
                };
                let result = data.findIndex(drow => {
                    return sqlobj.groupby.every(group => drow[group.column] == row[group.column]);
                });

                if (result == -1) {
                    data.push({});
                    return data.length - 1;
                }
                return result;
            }
            var index = groupby();

            for (let col of sqlobj.columns) {
                let name;
                switch(col.expr.type) {
                    case 'column_ref':
                        name = col.as || col.expr.column;
                        data[index][name] = row[col.expr.column];
                        break;
                    case 'aggr_func': 
                        name = col.as || col.expr.name.toUpperCase() + "(" + col.expr.args.expr.column + ")";
                        
                        switch(col.expr.name.toUpperCase()) {
                            case 'SUM':
                                if (data[index][name] === undefined) {
                                    data[index][name] = 0;
                                }
                                data[index][name] += row[col.expr.args.expr.column];
                                break;
                            case 'COUNT':
                                if (data[index][name] === undefined) {
                                    data[index][name] = 0;
                                }
                                data[index][name]++;
                                break;
                        }
                        break;
                }
            }
        } else {
            let result = {};
            for (let col of sqlobj.columns) {
                let name = col.as || (namespace ? col.expr.table + "." + col.expr.column : col.expr.column);
                result[name] = row[namespace ? col.expr.table + "." + col.expr.column : col.expr.column];
                if (result[name] === undefined) result[name] = null;
            }
            data.push(result);
        }
    }
    
    /**
     * Joins two tables
     * 
     * @param {Array<any>} dest The destination table, the join will perfer this table
     * @param {Array<any>} src The source table, the join will pick from this table
     * @param {boolean} includeAllDest If true, all rows in the destination table will be included in the results
     * @param {boolean} includeAllSrc If true, all rows in the source table will be included in the results
     * @param {boolean} namespace If true, the column names include their table name
     */
    join(dest, src, query, includeAllDest, includeAllSrc, namespace = false) {
        var rows = [];

        let destRows = dest.map(row => {
            return { used: false, row: row };
        });

        let srcRows = src.map(row => {
            return { used: false, row: row };
        });

        for (let destRow of destRows) {

            for (let srcRow of srcRows) {
                var bigrow = {}
                for (var k in destRow.row) { bigrow[k] = destRow.row[k]; }
                for (var k in srcRow.row) { bigrow[k] = srcRow.row[k]; }
                if (this.doWhere(query, bigrow, namespace)) {
                    rows.push(bigrow);
                    destRow.used = true;
                    srcRow.used = true;
                }
            }
        }

        if (includeAllDest) {
            destRows.filter(row => row.used == false).map(row => rows.push(row.row));
        }
        if (includeAllSrc) {
            srcRows.filter(row => row.used == false).map(row => rows.push(row.row));
        }

        return rows;
    }

    /**
     * Performs an SQL SELECT
     * 
     * @param {any} sqlobj
     */
    doSelect(sqlobj) {
        return new Promise((resolve, reject) => {
            let promises = [];
            let namespace = sqlobj.from.length > 1;
            
            for (let n = 0; n < sqlobj.from.length; n++) {
                promises.push(this.load(sqlobj.from[n].table));
            }
            Promise.all(promises).then((tableset) => {
                var tables = tableset.map((table, n) => {
                    var rows = Object.values(table);
                    var result = {
                        from: sqlobj.from[n],
                        name: sqlobj.from[n].table,
                        rows: []
                    }
                    if (namespace) {
                        result.rows = rows.map(row => {
                            var nsRow = {}
                            for (var key in row) {
                                nsRow[sqlobj.from[n].table + "." + key] = row[key];
                            }
                            return nsRow;
                        });
                    } else {
                        result.rows = rows;
                    }
                    return result
                });

                while (tables.length > 1) {
                    // take the second table and merge it into the first according to the join rules
                    switch(tables[1].from.join) {
                        case 'INNER JOIN':
                            tables[0].rows = this.join(tables[0].rows, tables[1].rows, tables[1].from.on, false, false, namespace);
                            break;
                        case 'LEFT JOIN':
                            tables[0].rows = this.join(tables[0].rows, tables[1].rows, tables[1].from.on, true, false, namespace);
                            break;
                        case 'RIGHT JOIN':
                            tables[0].rows = this.join(tables[0].rows, tables[1].rows, tables[1].from.on, false, true, namespace);
                            break;
                        case 'FULL JOIN':
                            tables[0].rows = this.join(tables[0].rows, tables[1].rows, tables[1].from.on, true, true, namespace);
                            break;
                    }
                    tables.splice(1,1);
                }

                // the join has been performed, now this is a big table treat it as such
                let result = [];
                
                let resultRows = tables[0].rows.filter(row => this.doWhere(sqlobj.where, row, namespace));

                if (sqlobj.orderby) {
                    resultRows.sort((a, b) => {
                        for (let orderer of sqlobj.orderby) {
                            let column = namespace ? orderer.expr.table + "." + orderer.expr.column : orderer.expr.column;
                            if (orderer.expr.type !== 'column_ref') {
                                throw new Error("ORDER BY only supported for columns, aggregates are not supported");
                            }

                            if (a[column] > b[column]) {
                                return orderer.type == 'ASC' ? 1 : -1;
                            }
                            if (a[column] < b[column]) {
                                return orderer.type == 'ASC' ? -1 : 1;
                            }
                        }
                        return 0;
                    });
                }

                resultRows.map(row => this.chooseFields(sqlobj, result, row, namespace));

                if (sqlobj.limit) {
                    if (sqlobj.limit.length !== 2) {
                        throw new Error("Invalid LIMIT expression: Use LIMIT [offset,] number");
                    }
                    let offs = parseInt(sqlobj.limit[0].value);
                    let len = parseInt(sqlobj.limit[1].value);
                    result = result.slice(offs, offs + len);
                }

                resolve(result);
            });
        });
    }

    /**
     * Performs an SQL UPDATE.
     * 
     * @param {function} resolve 
     * @param {function} reject 
     * @param {any} sqlobj
     * @returns {Promise<Array<string|number>>} 
     */
    doUpdate(sqlobj) {
        return new Promise((resolve, reject) => {
            this.load(sqlobj.table).then((table) => {
                let rows = [];
                let promises = [];
                let updateObj = {};

                for (let item of sqlobj.set) {
                    updateObj[item.column] = item.value.value;
                }

                for (let row_id in table) {
                    if (this.doWhere(sqlobj.where, table[row_id]) === true) {
                        promises.push(this.store(sqlobj.table, row_id, updateObj));
                        rows.push(row_id);
                    }
                }
                Promise.all(promises).then((values) => {
                    resolve(rows);
                }).catch((reason) => {
                    reject(reason);
                });
            });
        });
    }

    /**
     * Performs an SQL INSERT
     * 
     * @param {function} resolve 
     * @param {function} reject 
     * @param {any} sqlobj 
     * @returns {Promise<Array<string|number>}
     */
    doInsert(sqlobj) {
        return new Promise((resolve, reject) => {
            let rows = [];
            for (let i = 0; i < sqlobj.values.length; i++) {
                let data = {};
                for (let n = 0; n < sqlobj.columns.length; n++) {
                    data[sqlobj.columns[n]] = sqlobj.values[i].value[n].value;
                }
                
                rows.push(this.store(sqlobj.table, null, data));
            }
            Promise.all(rows).then((values) => {
                resolve(values);
            }).catch((reason) => {
                reject(reason);
            });
        });
    }

    /**
     * Performs an SQL DELETE.
     * 
     * @param {function} resolve 
     * @param {function} reject 
     * @param {any} sqlobj 
     * @returns {Promise<Array<string|number>>}
     */
    doDelete(sqlobj) {
        return new Promise((resolve, reject) => {
            this.load(sqlobj.from[0].table).then((table) => {
                let promises = [], rowIds = [];
                for (let row_id in table) { 
                    if (this.doWhere(sqlobj.where, table[row_id]) === true) {
                        rowIds.push(row_id);
                        promises.push(this.remove(sqlobj.from[0].table, row_id));
                    }
                }
                Promise.all(promises).then(() => {
                    resolve(rowIds);
                }).catch((reason) => {
                    reject(reason);
                });
            });
        });
    }

    /**
     * Performs an SQL CREATE
     * 
     * @param {any} sqlobj
     * @returns {Promise<boolean>}
     */
    doCreate(sqlobj) {
        return new Promise((resolve, reject) => {
            // create a new table definition
            let columns = [];
            let n = 0;
            for (var col of sqlobj.columns) {
                var column = {
                    name: col.name,
                    index: n++
                }

                switch (col.type.type.toUpperCase()) {
                    case 'CHAR':
                    case 'CHARACTER':
                        column.type = 'string';
                        column.pad = ' ';
                        column.length = parseInt(col.type.args[0]);
                        break;
                    case 'VARCHAR':
                        column.type = 'string';
                        column.length = parseInt(col.type.args[0]);
                        break;
                    case 'BINARY':
                    case 'VARBINARY':
                        column.type = 'binary';
                        column.length = parseInt(col.type.args[0]);
                        break;
                    case 'BOOLEAN':
                        column.type = 'boolean';
                        break;
                    case 'INTEGER':
                    case 'SMALLINT':
                    case 'BIGINT':                
                        column.type = 'integer';
                        break;
                    case 'DECIMAL':
                    case 'NUMERIC':
                    case 'FLOAT':
                    case 'REAL':
                    case 'DOUBLE':
                        column.type = 'float';
                        break;
                    case 'DATE':
                    case 'TIME':
                    case 'TIMESTAMP':
                        column.type = 'date';
                        break;
                    case 'INTERVAL':
                    case 'ARRAY':
                    case 'MULTISET':
                    case 'XML':
                        throw col.type.type.toUpperCase() + ' not yet supported';
                    case 'TEXT':
                        column.type = 'string';
                        break;
                }

                columns.push(column);
            }

            this.create(sqlobj.name.table, columns).then(success => resolve(success));
        });
    }

    /**
     * Performs an SQL DROP
     * 
     * @param {any} sqlobj
     * @returns {Promise<boolean>}
     */
    doDrop(sqlobj) {
        return this.drop(sqlobj.table);
    }

    /**
     * Runs the SQL statement
     * 
     * @param {string} sql 
     * @returns {Promise<array>} Promise of array of selected rows, updated rows, inserted rows, or deleted row Firebase keys
     * @memberof Firebase
     */
    runSQL(sql) {
        return new Promise((resolve, reject) => {
            this.ready().then(() => {
                // we are now authenticated
                let sqlobj;
                try {
                    sqlobj = parse(sql);
                } catch (err) {
                    // look for syntax not supported by SQL parser
                    sqlobj = false;
                    for (var key in ExtendedSyntax) {
                        if (ExtendedSyntax[key].re.test(sql)) {
                            sqlobj = ExtendedSyntax[key].convert(sql);
                        }
                    }
                    if (!sqlobj) {
                        return reject(err)
                    };
                }

                switch(sqlobj.type) {
                    case 'select':
                        this.doSelect(sqlobj).then(rows => resolve(rows)).catch(err => reject(err));
                        break;
                    case 'update':
                        this.doUpdate(sqlobj).then(ids => resolve(ids)).catch(err => reject(err));
                        break;
                    case 'insert':
                        this.doInsert(sqlobj).then(ids => resolve(ids)).catch(err => reject(err));
                        break;
                    case 'delete':
                        this.doDelete(sqlobj).then(ids => resolve(ids)).catch(err => reject(err));
                        break;
                    case 'create_table':
                        this.doCreate(sqlobj).then(success => resolve(success)).catch(err => reject(err));
                        break;
                    case 'drop_table':
                        this.doDrop(sqlobj).then(success => resolve(success)).catch(err => reject(err));
                        break;
                    default:
                        reject("Invalid SQL syntax: " + sqlobj.type);
                        break;
                }
            });
        });
    }

    /**
     * Executes the passed SQL
     * 
     * @param {string} sql 
     * @returns {Promise<array>} Promise of array of selected rows, updated rows, inserted rows, or deleted row Firebase keys
     * @memberof Firebase
     */
    execute(sql) {
        return this.runSQL(sql);
    }

    /**
     * Executes the passed SQL
     * 
     * @param {string} sql 
     * @returns {Promise<array>} Promise of array of selected rows, updated rows, inserted rows, or deleted row Firebase keys
     * @memberof Firebase
     */
    query(sql) {
        return this.runSQL(sql);
    }

    /* Abstract Functions */

    /**
     * Load all rows from a given table. Promise returns each row associated with
     * and index value that is string or integer
     * @param {string} table The table name to load rows from
     * @returns {Promise<{[key:string|number]:any}>} 
     */
    load(table) {
        throw "load must be overridden in the implementing class";
    }

    /**
     * Stores a row into the table
     * @param {string} table The name of the destination table
     * @param {number|string} index The array index or object key for the table row, null to insert
     * @param {any} row The data to store
     * @returns {number|string} Then index or object key which was stored
     */
    store(table, index, row) {
        throw "store must be overridden in the implementing class";
    }

    /**
     * Removes a row from the table
     * @param {string} table The name of the table
     * @param {number|string} index The array index or object key for the table row
     * @returns {Promise<any?>}
     */
    remove(table, index) {
        throw "remove must be overridden in the implementing class";
    }

    /**
     * Creates a new table
     * 
     * @param {string} table The table name to create
     * @param {Array<{name:string,index:number,type:string,length?:number,pad?:string}>} definition The table definition
     * @returns {Promise<boolean>}
     */
    create(table, definition) {
        throw "create must be overridden in the implementing class";
    }

    /**
     * Removes a table
     * 
     * @param {string} table The table name to remove
     * @returns {Promise<boolean>}
     */
    drop(table) {
        throw "drop must be overridden in the implementing class";
    }
    
    /**
     * Closes the connection
     * 
     * @returns {Promise<boolean>}
     */
    close() {
        throw "close must be overridden in the implementing class";
    }

    /**
     * Returns a promise when the driver is ready
     * 
     * @returns {Promise<boolean>}
     */
    ready() {
        throw "ready must be overridden in the implementing class";
    }
}

module.exports = AbstractDriver;