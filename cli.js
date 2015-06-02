#!/usr/bin/env node

var questions = require('./')({
  debug: false
});

var cmd = process.argv[2];

switch (cmd) {
  case 'search':
    questions
      .search({query: process.argv[3] || '*'})
      .pipe(process.stdout)
      .on('error', process.exit)
    break;
  case 'stats':
    questions
      .stats({query: process.argv[3] || '*'})
      .then(console.log)
    break;
  case 'download':
    questions.download();
    break;
  case 'create-index':
    questions.createIndex();
    break;
  case 'index':
    questions.index()
    break;
  case 'reindex':
    questions
      .deleteIndex()
      .then(questions.createIndex)
      .then(questions.index);

    break;
  case 'redo':
    questions
      .deleteIndex()
      .then(questions.createIndex)
      .then(questions.download)
      .then(questions.index);
    break;
  case 'cron':
    questions
      .download({cached: true})
      .then(questions.index);
    break;
  default:
    console.error('USAGE: hdo-parliament-questions download|create-index|index|reindex|redo|cron|search|stats');  
    process.exit(1);
}
