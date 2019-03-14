const benchmark = require('nodemark');

const very_large_str = (new Array((4 * 1024 * 1024) + 1).join('-'));
const very_large_buf = new Buffer(very_large_str);
const redis = require('../index');
const node_redis = require('redis');


const suite = {
  setup: {
    'asyncio': () => redis.connect(),
    'node_redis': () => node_redis.createClient()
  },
  shutdown: {
    'asyncio': (conn) => conn.close(),
    'node_redis': (conn) => conn.quit()
  },
  ops: [
    ['SET 4B', {
      'asyncio': (conn, cb) => conn.set('st', '1234').then(() => cb()),
      'node_redis': (conn, cb) => conn.set('st', '1234', cb)
    }],
    ['GET 4B', {
      'asyncio': (conn, cb) => conn.get('st').then(() => cb()),
      'node_redis': (conn, cb) => conn.get('st', cb)
    }],
    ['SET 4MiB [string]', {
      'asyncio': (conn, cb) => conn.set('st', very_large_str).then(() => cb()),
      'node_redis': (conn, cb) => conn.set('st', very_large_str, cb)
    }],
    ['GET 4MiB [string]', {
      'asyncio': (conn, cb) => conn.get('st').then(() => cb()),
      'node_redis': (conn, cb) => conn.get('st', cb)
    }],
    ['SET 4MiB [buffer]', {
      'asyncio': (conn, cb) => conn.set('st', very_large_buf).then(() => cb()),
      'node_redis': (conn, cb) => conn.set('st', very_large_buf, cb)
    }],
    ['GET 4MiB [buffer]', {
      'asyncio': (conn, cb) => conn.get('st').then(() => cb()),
      'node_redis': (conn, cb) => conn.get('st', () => cb())
    }],
  ]
};



async function main() {
  const setups = {};

  // run the setup (create the connections)
  for (let lib in suite.setup) {
    setups[lib] = await suite.setup[lib]();
  }

  // start the bench marking
  for (let [name, runs] of suite.ops) {
    console.log(`${name}\n---`)
    for (let [lib, run] of Object.entries(runs)) {
      const conn = setups[lib];
      const result = await benchmark((cb) => run(conn, cb));
      console.log(`  ${lib}: ${result}`)
    }
    console.log('');
  }

  // shutdown
  for (let lib in setups) {
    suite.shutdown[lib](setups[lib]);
  }


  // const conn = await redis.connect();

  // async function run_bench(name, fn) {
  //   const result = await benchmark(fn);
  //   console.log(`${name}:`, result);
  //   return result;
  // }

  // // let result = await benchmark((cb) => {
  // //   conn.hset('key', 'field', '1234').then(() => cb());
  // // });
  // // console.log('HSET 4B:', result);

  // await run_bench('SET 4B', (cb) => {
    
  // });

  // await run_bench('GET 4B', (cb) => {
  //   conn.get('st').then(() => cb());
  // });

  // await run_bench('SET 4MiB [string]', (cb) => {
  //   conn.set('st', very_large_str).then(() => cb());
  // });

  // await run_bench('GET 4MiB [string]', (cb) => {
  //   conn.get('st').then(() => cb());
  // });

  // await run_bench('SET 4MiB [Buffer]', (cb) => {
  //   conn.set('st', very_large_buf).then(() => cb());
  // });

  // await run_bench('GET 4MiB [Buffer]', (cb) => {
  //   conn.get('st').then(() => cb());
  // });

  // conn.close();
}
main();