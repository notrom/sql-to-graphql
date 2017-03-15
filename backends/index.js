'use strict';

var adapters = {
    mysql: require('./mysql'),
    postgres: require('./postgres'),
    pg: require('./postgres'),
    sqlite: require('./sqlite'),
    mssql: require('./mssql')
};

module.exports = function getBackendAdapter(db) {
    var backend = (db || '<not set>').toLowerCase();
    return adapters[backend];
};
