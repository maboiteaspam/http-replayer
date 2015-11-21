# http-replayer

node module which consumes a redis database
to replay http requests.


## install

    npm i maboiteaspam/http-replayer -g


## usage

    redis-server
    http-replayer [opts]

##### --rport=`6379`
Redis port

##### --rhost=`0.0.0.0`
Redis host

##### --ropts=``
Redis options as a stringified json object

##### --port=`8081`
Replay target http server port

##### --host=`127.0.0.1`
Replay target http server host


## more info

##### data pulling

The module use a main loop `updateCurrentJobs`.

This loop pull the whole keyspace at first,
it come from `blockKeys` redis `set`.

Later it won't fetch `blockKeys` until all known `keyspace`
are completed.

Once a `keyspace` is selected, the system invoke `pullJobs`
by range of `500` items.

`processJobs` is then executed to process
all selected jobs in parallel with a limit according to `jobQueueLength`,
 which defaults to `50`.

When `currentJobs` is empty, the systems `pullJobs` again,
and also tries to `doSomeCleanUp` to get a new `keyspace` id.

The system tries not to stuck on particular `keyspaces`,
and call `updateCurrentBlockKey` to distribute new `keyspace` to browse.


## need

##### optimizations

They are rooms too optimize.
Noticeable example is the redis reads/writes.

##### improve cookie support

Needs cookie support improvement.

## see also

- [http-flow-visualizer](https://github.com/maboiteaspam/http-flow-visualizer)
- [http-replayer](https://github.com/maboiteaspam/http-replayer)
