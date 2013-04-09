#!/usr/bin/node

var file = process.argv[2] || process.env.REDUCER;

if (!file)
  throw new Error('Must have input file as argument or env variable REDUCER.');

var stdin = process.stdin,
    stdout = process.stdout,
    code = require(file),
    current_key = null,
    current_vals = [];

function processLine(line) {
  if(line && line.trim().length > 0) {
    var s = line.trim().split('\t');
    if(s.length == 2) {
      var key = s[0].trim(),
          val = s[1];

      if (current_key == key) current_vals.push(val);
      else {
        if (current_key) {
          code(current_key, current_vals, out);
        }
        current_key = key;
        current_vals = [val];
      }
    }
  }
}

var data = '';
stdin.setEncoding('utf8');
stdin.resume();
stdin.on('data', function(chunk) {
  data += chunk;
  data = data.replace(/\r\n/g, '\n');
  while(data.indexOf('\n') > -1) {
    var i = data.indexOf('\n') + 1;
    processLine(data.slice(0,i));
    data=data.slice(i);
  }
});

stdin.on('end', function() {
  processLine(data);
  code(current_key, current_vals, out);
});

function out(key, value) {
  stdout.write(key + '\t' + value + '\n');
}

