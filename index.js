import fs from 'fs-promise';
import Promise from 'bluebird';
import requestRaw from 'request';
import _ from 'lodash';
import elasticsearch from 'elasticsearch';
import path from 'path';
import glob from 'glob-promise';
import moment from 'moment';
import mapping from './mapping';
import csv from 'csv';
import { ReadableSearch } from 'elasticsearch-streams';
import flatten from 'flat';

const request = Promise.promisify(requestRaw);

const TYPE = 'question';

const DEFAULTS = {
    elasticsearch: process.env.BOXEN_ELASTICSEARCH_URL || 'http://localhost:9200',
    index: 'hdo-parliament-questions',
    outputPath: path.resolve('./data'),
    concurrency: 10,
    currentSession: '2014-2015'
};

const IGNORE_PATTERN = /^(versjon|.+\.(versjon|.+dato|kjoenn))$/;

export default class Questions {
    constructor(opts = {})  {
        this.config = {...DEFAULTS, ...opts};

        this.es = new elasticsearch.Client({
            host: this.config.elasticsearch,
            log: this.config.debug ? 'trace' : null
        });
    }

    fetchSessionData(session, name, opts) {
        const url = `http://data.stortinget.no/eksport/${name}?sesjonid=${session}&format=json`;
        const out = path.join(this.config.outputPath, `${name}.${session}.json`);
        let notCached;

        return fs.stat(out).then(
            stat => !(opts && opts.cached && stat.isFile()),
            err  => true
        )
        .then(notCached => {
            if (notCached || session === this.config.currentSession) {
                return request({
                    url: url,
                    headers: {
                        'User-Agent': 'hdo-node-fetcher | holderdeord.no'
                    }
                })
                .spread((response, body) => {
                    if (response.statusCode !== 200) {
                        console.error(`response code ${response.statusCode} for ${url}`);
                    }

                    return fs.writeFile(out, body)
                        .then(() =>  console.log(url, '=>', out))
                        .catch(err => {
                            throw new Error(`unable to write ${url} to ${out}: ${err}`);
                        });
                });
            }
        });

    };

    download(opts) {
        return request('http://data.stortinget.no/eksport/sesjoner?format=json')
            .spread((response, body) => {
                var sessions = JSON.parse(body).sesjoner_liste.map(d => d.id);

                return Promise.map(sessions, (session) => {
                    console.log(session);

                    return Promise.join(
                        this.fetchSessionData(session, 'sporretimesporsmal', opts),
                        this.fetchSessionData(session, 'interpellasjoner', opts),
                        this.fetchSessionData(session, 'skriftligesporsmal', opts)
                    );
                });
            });
    };

    createIndex() {
        return this.es.indices.create({
            index: this.config.index,
            body: {
                mappings: {
                    question: mapping
                }
            }
        })
        .catch(console.error);
    };

    questionTypeFor(file) {
        var t = path.basename(file).match(/^(.+?)\./)[1];

        if (!t) {
            throw new Error(`unable to determine type from: ${file}`);
        }

        return t;
    };

    convertDates(obj) {
        if (Array.isArray(obj)) {
            return obj.map(::this.convertDates);
        } else if (obj !== null && typeof obj === 'object') {
            Object.keys(obj).forEach((key) => {
                obj[key] = this.convertDates(obj[key]);
            });

            return obj;
        } else if (typeof obj === 'string' && obj.indexOf('/Date(') === 0) {
            return moment(obj).format();
        } else {
            return obj;
        }
    };

    convertQuestion(sessionId, questionType, raw) {
        raw.sesjon_id = sessionId;
        raw.type_navn = questionType;

        this.convertDates(raw);

        return raw;
    };

    toBulkBody(questionType, questions) {
        return _.flatten(questions.map((doc) => {
            const id = [questionType, doc.id].join('-');

            return [
                {
                    index: {
                        _index: this.config.index,
                        _type: TYPE,
                        _id: id
                    }
                },
                doc
            ];
        }));
    };

    indexFile(file) {
        return fs.readFile(file, 'utf-8').then((content) => {
            const data = JSON.parse(content);
            const questions = data.sporsmal_liste.map(q =>
                this.convertQuestion(data.sesjon_id, this.questionTypeFor(file), q)
            );

            if (questions.length) {
                return this.es.bulk({
                    body: this.toBulkBody(this.questionTypeFor(file), questions)
                }).then(function() {
                    console.log('indexed', file);
                });
            }
        });
    };

    indexAll() {
        return glob(path.join(this.config.outputPath, '*.json')).then(files => {
            return Promise.map(files, ::this.indexFile, {
                concurrency: this.config.concurrency
            });
        });
    };

    deleteIndex() {
        return this.es.indices.delete({
            index: this.config.index
        })
        .catch(console.error);
    };

    search(opts) {
        const rs = new ReadableSearch((start, callback) => {
            this.es.search({
                index: this.config.index,
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

        const stringifier = csv.stringify({
            delimiter: '\t'
        });

        let columns;

        return rs
            .pipe(csv.transform(function(record) {
                const source = record._source;
                source.emne_liste = source.emne_liste.map(e => e.navn).sort().join(',');

                var flat = flatten(source);

                if (!columns) {
                    columns = Object.keys(flat).filter(function(k) {
                        return !k.match(IGNORE_PATTERN);
                    });
                    stringifier.write(columns);
                }

                return columns.map(function(k) {
                    return flat[k];
                });
            }))
            .pipe(stringifier);
    };

    stats(opts) {
        return this.es.search({
            index: this.config.index,
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

            rows = _.sortBy(rows, function(r) {
                return r[0];
            });

            return Promise.promisify(csv.stringify)(rows, {
                delimiter: '\t',
                columns: ['session', 'count'],
                header: true
            });
        });
    };
}
