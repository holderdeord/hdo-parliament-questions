var Promise = require('bluebird');
var request = Promise.promisify(require('request'));
var fs = Promise.promisifyAll(require('fs'));
var _ = require('lodash');
var elasticsearch = require('elasticsearch');
var path = require('path');
var xtend = require('xtend');
var glob = Promise.promisify(require('glob'));
var moment = require('moment');
var mapping = require('./mapping');
var csv = require('csv');
var ReadableSearch = require('elasticsearch-streams').ReadableSearch;
var flatten = require('flat');

var TYPE = 'question';

var DEFAULTS = {
    elasticsearch: process.env.BOXEN_ELASTICSEARCH_URL || 'http://localhost:9200',
    index: 'hdo-parliament-questions',
    outputPath: path.resolve('./data'),
    concurrency: 10,
    currentSession: '2014-2015'
};

var IGNORE_PATTERN = /^(versjon|.+\.(versjon|.+dato|kjoenn))$/;

module.exports = function(opts) {
  var config = xtend(DEFAULTS, opts || {});

  var es = new elasticsearch.Client({
    host: config.elasticsearch,
    log: config.debug ? 'trace' : null
  });

  var fetchSessionData = function(session, name, opts) {
    var url = 'http://data.stortinget.no/eksport/' + name + '?sesjonid=' + session + '&format=json';
    var out = path.join(config.outputPath, name + '.' + session + '.json');

    return fs.stat(out, function(err, stat) {
      var notCached = err || !(opts && opts.cached && stat.isFile());

      if (notCached || session === config.currentSession) {
        return request({
            url: url,
            headers: {'User-Agent': 'hdo-node-fetcher | holderdeord.no'}
          })
          .spread(function(response, body) {
            if (response.statusCode !== 200) {
              console.error('response code ' + response.statusCode + ' for ' + url);
            }

            return fs.writeFileAsync(out, body)
              .then(function() {
                console.log(url, '=>', out);
              }).catch(function(err) {
                throw new Error('unable to write ' + url + ' to ' + out + ': ' + err);
              });
          });
      }
    });
  };

  var download = function(opts) {
    return request('http://data.stortinget.no/eksport/sesjoner?format=json')
      .spread(function(response, body) {
        var sessions = JSON.parse(body).sesjoner_liste.map(function(d) { return d.id; });

        return Promise.map(sessions, function(session) {
          console.log(session);

          return Promise.join(
            fetchSessionData(session, 'sporretimesporsmal', opts),
            fetchSessionData(session, 'interpellasjoner', opts),
            fetchSessionData(session, 'skriftligesporsmal', opts)
          );
        });
      });
  };

  var createIndex = function(argument) {
    return es.indices.create({
      index: config.index,
      body: {
        mappings: {
          question: mapping
        }
      }
    });
  };

  var questionTypeFor = function(file) {
    var t = path.basename(file).match(/^(.+?)\./)[1];

    if (!t) {
      throw new Error('unable to determine type from: ' + file);
    }

    return t;
  };

  var convertDates = function(obj) {
    if (Array.isArray(obj)) {
      return obj.map(convertDates);
    } else if (obj !== null && typeof obj === 'object') {
      Object.keys(obj).forEach(function(key) {
        obj[key] = convertDates(obj[key]);
      });

      return obj;
    } else if (typeof obj === 'string' && obj.indexOf('/Date(') === 0) {
      return moment(obj).format();
    } else {
      return obj;
    }
  };

  var convertQuestion = function(sessionId, questionType, raw) {
    raw.sesjon_id = sessionId;
    raw.type_navn = questionType;

    convertDates(raw);

    return raw;
  };

  var toBulkBody = function(questionType, questions) {
    return _.flatten(questions.map(function(doc) {
      var id = [questionType, doc.id].join('-');

      return [
        {index: { _index: config.index, _type: TYPE, _id: id}},
        doc
      ];
    }));
  };

  var indexFile = function(file) {
    return fs.readFileAsync(file, 'utf-8').then(function(content) {
      var data = JSON.parse(content);
      var questions = data.sporsmal_liste.map(function(q) {
        return convertQuestion(data.sesjon_id, questionTypeFor(file), q);
      });

      if (questions.length) {
        return es.bulk({body: toBulkBody(questionTypeFor(file), questions)}).then(function() {
          console.log('indexed', file);
        });
      }
    });
  };

  var indexAll = function() {
    return glob(path.join(config.outputPath, '*.json')).then(function(files) {
      return Promise.map(files, indexFile, {concurrency: config.concurrency});
    });
  };

  var deleteIndex = function() {
    return es.indices.delete({index: config.index});
  };

  var search = function(opts) {
    var rs = new ReadableSearch(function(start, callback) {
       es.search({
        index: config.index,
        body: {
          query: {
            query_string: {
              query: opts.query
            }
          },
          from: start,
          size: 100
        }
      }, callback);
    });

    var stringifier = csv.stringify({delimiter: '\t'});

    var columns;

    return rs
      .pipe(csv.transform(function(record) {
        var source = record._source;
        source.emne_liste = source.emne_liste.map(function(e) { return e.navn; }).sort().join(',');

        var flat = flatten(source);

        if (!columns) {
          columns = Object.keys(flat).filter(function(k) { return !k.match(IGNORE_PATTERN); });
          stringifier.write(columns);
        }

        return columns.map(function(k) { return flat[k]; });
      }))
      .pipe(stringifier);
  };

  var stats = function(opts) {
    return es.search({
      index: config.index,
      body: {
        query: {
          query_string: {
            query: opts.query
          }
        },
        aggregations: {
          sessionCounts: {
            terms: {
              field: 'sesjon_id',
              size: 200,
            }
          }
        }
      }
    }).then(function(response) {
      var rows = response.aggregations.sessionCounts.buckets.map(function(bucket) {
        return [bucket.key, bucket.doc_count];
      });

      rows = _.sortBy(rows, function(r) { return r[0]; });

      return Promise.promisify(csv.stringify)(rows, {
        delimiter: '\t',
        columns: ['session', 'count'],
        header: true
      });
    });
  };

  return {
    download: download,
    createIndex: createIndex,
    deleteIndex: deleteIndex,
    index: indexAll,
    search: search,
    stats: stats
  };
};