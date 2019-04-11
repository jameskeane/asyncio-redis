const very_large_str = (new Array((4 * 1024 * 1024) + 1).join('-'));
const very_large_buf = new Buffer(very_large_str);
const redis = require('../index');



async function run_set_small(times=100000) {
  const conn = await redis.connect();

  for (let i = 0; i < times; i++)
    await conn.set('st', '1234');

  conn.close();
}


async function run_get_large(times=10000) {
  const conn = await redis.connect();
  await conn.set('st', very_large_buf);

  for (let i = 0; i < times; i++)
    await conn.get('st');

  conn.close();
}

// run_set_small();
run_get_large();
