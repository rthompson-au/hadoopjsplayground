#!/usr/bin/node

var file = process.argv[2] || process.env.MAPPER;

if (!file)
  throw new Error('Must have input file as argument or env variable MAPPER.');

var stdin = process.stdin,
    stdout = process.stdout,
    code = require(file);

var data = '';
stdin.setEncoding('utf8');
stdin.resume();
stdin.on('data', function(chunk) {
  data += chunk;
  data = data.replace(/\r\n/g, '\n');
  while(data.indexOf('\n') > -1) {
    var i = data.indexOf('\n') + 1;
    code(data.slice(0,i), out);
    data=data.slice(i);
  }
});

stdin.on('end', function() {
  code(data, out);
});

function out(key, value) {
  stdout.write(key + '\t' + value + '\n');
}

