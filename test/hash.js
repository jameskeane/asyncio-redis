const assert = require('assert');
const { RedisClient, ...redis} = require('../index');



describe('Redis Hash Functions', () => {
  /**
   * @type {RedisClient}
   */
  let conn;

  beforeEach(async () => {
    conn = await redis.connect();
  });

  afterEach(async () => {
    await conn.del('hash_tests');
    conn.close();
  })

  describe('HDEL', () => {
    beforeEach(async () => {
      await Promise.all([
        conn.hset('hash_tests', 'f1', 'v1'),
        conn.hset('hash_tests', 'f2', 'v2'),
        conn.hset('hash_tests', 'f3', 'v3'),
      ]);
      assert.deepStrictEqual(await conn.hkeys('hash_tests'), ['f1', 'f2', 'f3']);
    });

    it('should remove a field from the hash', async() => {
      assert.equal(await conn.hdel('hash_tests', 'f2'), 1);
      assert.deepStrictEqual(await conn.hkeys('hash_tests'), ['f1', 'f3']);
    });

    it('should remove multiple fields if provided at once from the hash', async () => {
      assert.equal(await conn.hdel('hash_tests', 'f2', 'f1'), 2);
      assert.deepStrictEqual(await conn.hkeys('hash_tests'), ['f3']);
    });
  });

  describe('HEXISTS', () => {
    it('returns 0 if the field doesn\'t exist', async () => {
      await conn.hset('hash_tests', 'f2', 'v2'),
      assert.equal(await conn.hexists('hash_tests', 'f3'), 0);
      assert.equal(await conn.hexists('no-key', 'f1'), 0);
    });
    it('returns 1 if the field exists', async () => {
      await conn.hset('hash_tests', 'f2', 'v2'),
      assert.equal(await conn.hexists('hash_tests', 'f2'), 1);
    });
  });

  describe('HGET', () => {
    it('should return null if the key doesn\'t exist', async () => {
      assert.equal(await conn.hget('no-key', 'f'), null);
    });

    it('should return null if the field doesn\'t exist on a hash', async () => {
      await conn.hset('hash_tests', 'f1', 'v');
      assert.equal(await conn.hget('hash_tests', 'no-field'), null);
    });

    it('should return the current value of a field', async () => {
      await conn.hset('hash_tests', 'f1', 'v');
      assert.equal(await conn.hget('hash_tests', 'f1'), 'v');
    });
  });

  describe('HGETALL', () => {
    it('should return an alternating list of field, values for the hash', async() => {
      await Promise.all([
        conn.hset('hash_tests', 'f1', 'v1'),
        conn.hset('hash_tests', 'f2', 'v2'),
        conn.hset('hash_tests', 'f3', 'v3'),
      ]);

      const res = await conn.hgetall('hash_tests');
      assert.deepStrictEqual(res, ['f1', 'v1', 'f2', 'v2', 'f3', 'v3']);
    });
  });

  describe('HINCRBY', () => {
    it('should add to the value of an existing hash field by the specified increment', async () => {
      await conn.hset('hash_tests', 'f1', 5);
      assert.strictEqual(await conn.hincrby('hash_tests', 'f1', 10), 15);
      assert.strictEqual(await conn.hget('hash_tests', 'f1'), '15');
      // should also support negative numbers
      assert.strictEqual(await conn.hincrby('hash_tests', 'f1', -7), 8);
      assert.strictEqual(await conn.hget('hash_tests', 'f1'), '8');
    });
  });

  describe('HINCRBYFLOAT', () => {
    it('should add to the value of an existing hash field by the specified floating point increment', async () => {
      await conn.hset('hash_tests', 'f1', 10.50);
      assert.strictEqual(await conn.hincrbyfloat('hash_tests', 'f1', 0.1), '10.6');
      assert.strictEqual(await conn.hincrbyfloat('hash_tests', 'f1', -5.6), '5');
      assert.strictEqual(await conn.hincrbyfloat('hash_tests', 'f1', 5.0e3), '5005');
    });

    it('will return an error if the field is the wrong type', async () => {
      await conn.hset('hash_tests', 'f1', 'test');

      let f = () => {};
      try {
        await conn.hincrbyfloat('hash_tests', 'f1', 0.1);
      } catch (e) {
        f = () => { throw e; };
      } finally {
        assert.throws(f, 'ERR hash value is not a float');
      }
    });
  });

  describe('HKEYS', () => {
    it('should return an array of the field names of the given hash', async () => {
      await Promise.all([
        conn.hset('hash_tests', 'field1', 'value1'),
        conn.hset('hash_tests', 'field2', 'value2'),
        conn.hset('hash_tests', 'other', 'value2'),
      ]);

      const res = await conn.hkeys('hash_tests');
      assert.deepStrictEqual(res, ['field1', 'field2', 'other']);
    });

    it('should return an empty array if the key does not exist', async () => {
      const res = await conn.hkeys('no-key');
      assert.deepStrictEqual(res, []);
    });
  });

  describe('HLEN', () => {
    it('should return the number of fields contained in the hash', async () => {
      assert.strictEqual(await conn.hlen('hash_tests'), 0);

      await Promise.all([
        conn.hset('hash_tests', 'f1', 'value1'),
        conn.hset('hash_tests', 'f2', 'value2'),
      ]);

      assert.strictEqual(await conn.hlen('hash_tests'), 2);
      await conn.hset('hash_tests', 'f3', 'value3');
      assert.strictEqual(await conn.hlen('hash_tests'), 3);
      await conn.hdel('hash_tests', 'f1', 'f2');
      assert.strictEqual(await conn.hlen('hash_tests'), 1);
      await conn.hdel('hash_tests', 'f3');
      assert.strictEqual(await conn.hlen('hash_tests'), 0);
    });
  });

  describe('HMGET', () => {
    it('should return the values of the specified keys of the hash', async () => {
      await conn.hmset('hash_tests', 'f1', 'v1', 'f2', 'v2', 'f3', 'v3'),

      assert.deepStrictEqual(
          await conn.hmget('hash_tests', 'f2', 'f1', 'no-field', 'f3'),
          ['v2', 'v1', null, 'v3']);
    });
  });
  
  describe('HMSET', () => {
    it('should set the fields and values provided to the hash', async () => {
      assert.deepStrictEqual(await conn.hgetall('hash_tests'), []);
      assert.strictEqual(
          await conn.hmset('hash_tests', 'f1', 'v1', 'f2', 'v2', 'f3', 'v3'),
          'OK');
      assert.deepStrictEqual(
          await conn.hgetall('hash_tests'),
          ['f1', 'v1', 'f2', 'v2', 'f3', 'v3']);
    });
  });

  describe('HSET', () => {
    it('should return 1 if the key is not already a hash', async () => {
      let res = await conn.hset('hash_tests', 'field1', 'value1');
      assert.equal(res, 1);
      res = await conn.hset('hash_tests', 'field2', 'value2');
      assert.equal(res, 1);
    });

    it('should return 0 if the hash already exists and is updated', async () => {
      let res = await conn.hset('hash_tests', 'field1', 'value1');
      assert.equal(res, 1);
      res = await conn.hset('hash_tests', 'field1', 'value2');
      assert.equal(res, 0);
    });
  });

  describe('HSETNX', () => {
    it('will set a field value if it does not exist, otherwise it will do nothing', async () => {
      await conn.hset('hash_tests', 'f1', 'v1');
      assert.strictEqual(await conn.hsetnx('hash_tests', 'f2', 'v2'), 1);
      assert.strictEqual(await conn.hget('hash_tests', 'f2'), 'v2');
      assert.strictEqual(await conn.hsetnx('hash_tests', 'f1', 'nope'), 0);
      assert.strictEqual(await conn.hget('hash_tests', 'f1'), 'v1');
    });
  });

  describe('HSTRLEN', () => {
    it('returns the length of the string stored at the field in the hash', async () => {
      assert.equal(await conn.hmset(
          'hash_tests', 'f1', 'HelloWorld', 'f2', '99', 'f3', '-256'), 'OK');
      assert.strictEqual(await conn.hstrlen('hash_tests', 'f1'), 10);
      assert.strictEqual(await conn.hstrlen('hash_tests', 'f2'), 2);
      assert.strictEqual(await conn.hstrlen('hash_tests', 'f3'), 4);
    });
  });

  describe('HVALS', () => {
    it('returns the values for the given hash', async () => {
      assert.equal(await conn.hmset(
          'hash_tests', 'f1', 'HelloWorld', 'f2', '99', 'f3', '-256'), 'OK');
      assert.deepStrictEqual(await conn.hvals('hash_tests'),
          ['HelloWorld', '99', '-256']);
    });
  });
});
