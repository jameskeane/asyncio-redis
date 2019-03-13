const benchmark = require('nodemark');



async function main() {
  const redis = require('../index');
  const conn = await redis.connect();

  let result = await benchmark((cb) => {
    conn.hset('key', 'field', '1234').then(() => cb());
  });
  console.log('HSET 4B:', result);


  result = await benchmark((cb) => {
    conn.set('st', '1234').then(() => cb());
  });
  console.log('SET 4B:', result);

  conn.close();
}


async function main_node_redis() {
  // const node_redis = require('redis');

  // const client = node_redis.createClient();
  // let result = await benchmark((cb) => {
  //   client.hset('key', 'field', '1234', cb);
  // });
  // console.log('HSET 4B:', result);

  // result = await benchmark((cb) => {
  //   client.set('st', '1234', cb);
  // });
  // console.log('SET 4B:', result);
  // client.quit();
}


main().then(() => {
  main_node_redis();
})