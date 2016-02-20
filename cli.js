#!./node_modules/.bin/babel-node

import Questions from './';

process.on('unhandledRejection', console.error);

const questions = new Questions({
    debug: false
});

var cmd = process.argv[2];

switch (cmd) {
    case 'search':
        questions
            .search({
                query: process.argv[3] || '*'
            })
            .pipe(process.stdout)
            .on('error', process.exit)
        break;
    case 'stats':
        questions
            .stats({
                query: process.argv[3] || '*'
            })
            .then(console.log)
            .catch(console.error)
        break;
    case 'download':
        questions
            .download()
            .catch(console.error);
        break;
    case 'create-index':
        questions
            .createIndex()
            .catch(console.error)
        break;
    case 'index':
        questions
            .index()
            .catch(console.error)
        break;
    case 'reindex':
        questions
            .deleteIndex()
            .then(() => questions.createIndex())
            .then(() => questions.indexAll())
            .catch(console.error)
        break;
    case 'redo':
        questions
            .deleteIndex()
            .then(() => questions.createIndex())
            .then(() => questions.download())
            .then(() => questions.indexAll())
            .catch(console.error)
        break;
    case 'cron':
        questions
            .download({
                cached: true
            })
            .then(() => questions.indexAll())
            .catch(console.error)
        break;
    default:
        console.error('USAGE: hdo-parliament-questions download|create-index|index|reindex|redo|cron|search|stats');
        process.exit(1);
}