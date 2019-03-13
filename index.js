const redis = require('./lib/redis');


async function main() {
  const client = await redis.connect();

  console.log(await client.hset('test', 'key1', 'value1'));
  console.log(await client.hset('test', 'key3', 'value3'));
  console.log(await client.hget('test', 'key1'));  // == 'value1'
  console.log(await client.hget('test', 'key3'));  // == 'value3'
  console.log(await client.hget('test', 'key2'));  // == null
  console.log(await client.hget('test2', 'key2'));  // == null

  console.log(await client.hkeys('test'));  // == ['key1']
  console.log(await client.hkeys('emptykey'));  // == []


  // const info = await client.info();

  // console.log(info);
}
main();