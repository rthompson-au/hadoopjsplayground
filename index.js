var fs = require('fs'),
    cp = require('child_process'),
    mkdirp = require('mkdirp'),
    path = require('path');

module.exports = exports = function(data, mapper, reducer, cb) {
  var time = Date.now(),
      dir = '/tmp/js_hadoop_' + time;

  mkdirp(dir, function(err) {
    if (err) throw new Error(err);
    var mfile = path.join(dir, 'mapper'),
        rfile = path.join(dir, 'reducer'),
        dfile = path.join(dir, 'data'),
        mh = fs.openSync(mfile, 'w'),
        rh = fs.openSync(rfile, 'w'),
        dh = fs.openSync(dfile, 'w');

    // Write mapper/reducer and make executable
    fs.writeSync(mh, createMapper(mapper));
    fs.chmodSync(mfile, 0755);
    fs.writeSync(rh, toString(reducer));
    fs.chmodSync(rfile, 0755);

    // Write data to file
    fs.writeSync(dh, makeDataFile(data));

    // Call hadoop
    var call = cp.spawn(path.join(__dirname, 'run.sh'), [
      mfile,
      rfile,
      dfile,
      path.join(dir, 'output')
    ], { cwd: __dirname });

    // Ignoring stdout currently
    call.stdout.on('data', function(data) {
      //process.stdout.write(data);
    });

    call.stderr.on('data', function(data) {
      //process.stdout.write(data);
    });

    call.on('error', function(e) {
      throw new Error(e);
    });

    call.on('close', function(code) {
      if (code !== 0) throw new Error('Nonzero exit code: %s', code);

      // Must get data back now
      fs.readdir(path.join(dir, 'output'), function(err, files) {
        if (err) throw new Error(err);

        var results = [];
        for (var i in files) {
          var f = files[i];
          if (f.match(/^part-/)) {
            var data = fs.readFileSync(path.join(dir, 'output', f), 'utf8')
                         .replace(/\r\n/g, '\n'),
                lines = data.split('\n');
            for (var j in lines) {
              var l = lines[j];
              if (l == '') continue;
              var sp = l.split('\t'),
                  d = {
                    key: sp.shift(),
                    value: sp.join('\t')
                  }
              results.push(d);
            }
          }
        }
        cb(results);
      });
    });

  });
}

function toString(fn) {
  return 'module.exports = exports = ' + fn.toString() + ';';
}

function createMapper(mapper) {
  return 'module.exports = exports = ' + 
         'function(line, out) {\n' +
          '\ttry{ var d = JSON.parse(line);}\n' +
          '\tcatch(e) { return; }\n' +
          '\tvar m = ' + mapper.toString() + ';\n' +
          '\tm(d, out);\n' + 
        '};';
}


function makeDataFile(data) {
  var str = '';
  for (var i in data) {
    str += JSON.stringify(data[i]) + '\n';
  }
  return str;
}
