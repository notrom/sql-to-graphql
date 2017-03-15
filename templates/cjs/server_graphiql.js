'use strict';

var Hapi = require('hapi');
var GraphQL = require('hapi-graphql');
var schema = require('./schema');

var server = new Hapi.Server();
server.connection({ port: 3000 });

server.register({
  register: GraphQL,
  options: {
    query: {
      schema: schema,
      graphiql: true
    },
    route: {
      path: '/graphql',
      config: {}
    }
  }
}, function() {
    server.start(function() {
      console.log('Server running at:', server.info.uri);
    })
});