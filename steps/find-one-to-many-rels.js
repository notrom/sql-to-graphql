'use strict';

var async = require('async');
var after = require('lodash/function/after');
var find = require('lodash/collection/find');
var camelCase = require('lodash/string/camelCase');
var capitalize = require('lodash/string/capitalize');
var pluralize = require('pluralize');

module.exports = function findOneToManyRelationships(adapter, models, callback) {
    var tasks = Object.keys(models).reduce(function(tasklist, model) {
        tasklist[model] = function(cb) {
            findRelationships(adapter, models[model], models, cb);
        };
        return tasklist;
    }, {});

    async.parallel(tasks, callback);
};

function findRelationships(adapter, model, models, callback) {
    if (!model.references.length) {
        return setImmediate(callback, null, model);
    }

    var done = after(model.references.length, callback);
    model.references.forEach(function(ref) {
        var referenceColumn = getUnaliasedField(ref.refField, model);
        // cheating!
        //return done(null, model);
        //adapter.hasDuplicateValues(model.table, referenceColumn, function(err, hasDupes) {
        adapter.hasDuplicateValues(model.table, ref.refField, function(err, hasDupes) {
            if (err) {
                return callback(err);
            }

            if (!hasDupes) {
                return done(null, model);
            }

            var reverseRefs = ref.model.listReferences;
            var refName = camelCase(pluralize(model.name));
            var description = pluralize(model.name) + ' belonging to this ' + ref.model.name;
            if (find(reverseRefs, { field: refName }) || ref.model.fields[refName]) {
                // @TODO find a better name algo resolve mechanism
                // `thread_id` should naturally be `threads`, while `old_thread_id` should be.. something else
                refName += capitalize(camelCase(referenceColumn)).replace(/Id$/, '');
                description += '..? (' + referenceColumn + ')';
            }

            reverseRefs.push({
                model: model,
                description: description,
                field: refName,
                refField: ref.refField,
                isList: true
            });

            done(null, model);
        });
    });
}


// here
function getUnaliasedField(field, model) {
    for (var unaliased in model.aliasedFields) {
        if (model.aliasedFields[unaliased] === field) {
            return unaliased;
        }
    }
}
