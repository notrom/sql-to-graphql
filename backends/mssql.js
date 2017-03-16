/* eslint camelcase: 0 */
'use strict';

var knex = require('knex');
var uniq = require('lodash/array/uniq');
var pluck = require('lodash/collection/pluck');
var mapKeys = require('lodash/object/mapKeys');
var contains = require('lodash/collection/includes');
var camelCase = require('lodash/string/camelCase');
var undef;

module.exports = function mssqlBackend(opts, cb) {
    //var up = '';
    //if (opts.user !== 'root' || opts.password) {
    //    up = opts.user + ':' + opts.password + '@';
    //}

    //var port = opts.port === 1433 ? '' : ':' + opts.port;
    //var conString = 'postgres://' + up + opts.host + port + '/' + opts.db;

    var mssql = knex({
        client: 'mssql',
        connection: {
            server: opts.host,
            port: opts.port,
            user : opts.user,
            password : opts.password,
            database : opts.db
        }
    });

    process.nextTick(cb);

    return {
        getTables: function(tableNames, tblCb) {
            var matchAll = tableNames.length === 1 && tableNames[0] === '*';
            mssql('INFORMATION_SCHEMA.TABLES')
                .distinct('TABLE_NAME')
                .distinct('TABLE_SCHEMA')
                .where({
                    table_catalog: opts.db,
                    table_type: 'BASE TABLE'
                })
                .then(function(tbls) {
                    // We want to get the schema name too
                    var schemaTables = tbls.map(function(tbl){
                        return tbl['TABLE_SCHEMA'] + '.' + tbl['TABLE_NAME'];
                    });
                    console.log(schemaTables);
                    tbls = schemaTables;
                    //tbls = pluck(tbls, 'TABLE_NAME');
                    if (!matchAll) {
                        tbls = tbls.filter(function(tbl) {
                            return contains(tableNames, tbl);
                        });
                    }
                    tblCb(null, tbls);
                })
                .catch(tblCb);
        },

        getTableComment: function(tableName, tblCb) {
            tblCb(null, '');
        },

        getTableStructure: function(tableName, tblCb) {
            var schemaTable = tableName.split(".");
            mssql.select('TABLE_NAME', 'COLUMN_NAME', 'ORDINAL_POSITION', 'IS_NULLABLE', 'DATA_TYPE', 'DOMAIN_NAME')
                .from('information_schema.COLUMNS AS c')
                .where({
                    table_catalog: opts.db,
                    table_name: schemaTable[1],
                    table_schema: schemaTable[0]
                })
                .orderBy('ordinal_position', 'asc')
                .catch(tblCb)
                .then(function(columns) {
                    var enumQueries = uniq(columns.filter(function(col) {
                        return col.DATA_TYPE === 'USER-DEFINED';
                    }).map(function(col) {
                        return 'enum_range(NULL::' + col.DOMAIN_NAME + ') AS ' + col.DOMAIN_NAME;
                    })).join(', ');

                    mssql.raw('SELECT ' + (enumQueries || '1 AS "1"')).then(function(enumRes) {
                        var enums = enumRes[0];

                        var subQuery = mssql.select('constraint_name')
                            .from('information_schema.table_constraints')
                            .where({
                                table_catalog: opts.db,
                                table_name: schemaTable[1],
                                table_schema: schemaTable[0],
                                constraint_type: 'PRIMARY KEY'
                            });

                        mssql.first('column_name AS primary_key')
                            .from('information_schema.key_column_usage')
                            .where({
                                table_catalog: opts.db,
                                table_name: schemaTable[1],
                                table_schema: schemaTable[0],
                                constraint_name: subQuery
                            })
                            .then(function(pk) {
                                var pkCol = (pk || {}).primary_key;
                                columns = columns.map(function(col) {
                                    var isUserDefined = col.DATA_TYPE === 'USER-DEFINED';
                                    col.columnKey = col.COLUMN_NAME === pkCol ? 'PRI' : null;
                                    col.columnType = isUserDefined ? enums[col.udt_name] : null;
                                    return col;
                                });

                                tblCb(null, (columns || []).map(camelCaseKeys));
                            });
                    }).catch(tblCb);
                });
        },

        hasDuplicateValues: function(table, column, callback) {
            mssql
                .select(column)
                .from(table)
                .groupBy(column)
                .havingRaw('count(' + column + ') > 1')
                .limit(1)
                .catch(callback)
                .then(function(info) {
                    callback(null, (info || []).length > 0);
                });
        },

        close: function(tblCb) {
            mssql.destroy(tblCb);
        }
    };
};

function camelCaseKeys(obj) {
    return mapKeys(obj, function(val, key) {
        return camelCase(key);
    });
}
