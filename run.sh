#/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [ $# -ne 4 ]
then
  echo "Four arguments require: run.js <mapper> <reducer> <input> <output>"
  echo "Also, you must use absolute paths"
  exit
fi

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

hadoop jar /opt/local/share/hadoop/contrib/streaming/hadoop-streaming-1.0.4.jar \
  -cmdenv MAPPER=$1 \
  -cmdenv REDUCER=$2 \
  -mapper $DIR/run_mapper.js \
  -reducer $DIR/run_reducer.js \
  -input $3 \
  -output $4 \
  -file $1 \
  -file $2 \
  -file $DIR/run_mapper.js \
  -file $DIR/run_reducer.js \
