const assert = require('assert');
const { RedisClient, ...redis} = require('../index');
const { asyncThrows } = require('./util');



describe('Redis Hash Functions', () => {
  /**
   * @type {RedisClient}
   */
  let conn;
  beforeEach(async () => {
    conn = await redis.connect();
    await conn.xadd('stream_tests', { 'key': 'val' });
  });

  afterEach(async () => {
    await conn.del('stream_tests');
    conn.close();
  });

  describe('XADD', () => {
    it('will append an entry onto a stream', async () => {
      await conn.xadd('stream_tests', { 'e1': 'v' });
    });

    it('accepts an \'id\' parameter to append an entry', async () => {
      const id = await conn.xadd('stream_tests', { 'e2': 'v2' }, { id:  '99999999999999-0' });
      assert.equal(id, '99999999999999-0');
    });

    it('accepts a \'maxlen\' parameter (as a number) to truncate the stream.', async () => {
      await conn.xadd('stream_tests', { 'e2': 'v2' }, { maxlen: 1024 });
    });

    it('accepts a \'maxlen_exact\' parameter, that truncates the stream exactly', async () => {
      await conn.xadd('stream_tests', { 'e2': 'v2' }, { maxlen_exact: 1024 });
    });
  });

  describe('XGROUP', () => {
    describe('the \'create\' subcommand', () => {
      it('can create a new consumer group for a stream.', async () => {
        assert.equal(await conn.xgroup('create', 'stream_tests', 'group1', '$'), 'OK');
        assert.equal(await conn.xgroup('create', 'stream_tests', 'group2', 0), 'OK');
      });

      it('will throw an error if attempting to create a group that already exists', async () => {
        assert.equal(await conn.xgroup('create', 'stream_tests', 'group-exists', '$'), 'OK');
        await asyncThrows(() => conn.xgroup('create', 'stream_tests', 'group-exists', '$'),
          /BUSYGROUP Consumer Group name already exists/);
      });
    });

    describe('the \'destroy\' subcommand', () => {
      it('can delete a previously created consumer group', async () => {
        assert.equal(await conn.xgroup('create', 'stream_tests', 'group_to_destroy', '$'), 'OK');
        assert.equal(await conn.xgroup('destroy', 'stream_tests', 'group_to_destroy'), 1);
      });

      it('will return the number of consumer groups that were destroyed', async () => {
        assert.equal(await conn.xgroup('create', 'stream_tests', 'group_to_destroy', '$'), 'OK');
        assert.equal(await conn.xgroup('destroy', 'stream_tests', 'group_to_destroy'), 1);
        // already deleted so 0
        assert.equal(await conn.xgroup('destroy', 'stream_tests', 'group_to_destroy'), 0);
      });
    });

    it('the \'delconsumer\' subcommand');
  });

  describe('XREAD', () => {
    it('will read from multiple streams at once', async () => {
      await conn.xadd('stream_tests2', { 'key': 'val2', 'key2': '123' }, { id:  '333333333-0' });
      await conn.xadd('stream_tests3', { 'key': 'val3' }, { id:  '222222222-0' });

      try {
        const res = await conn.xread({ 'stream_tests2': '0-0', 'stream_tests3': '0-0' });
        assert.deepStrictEqual(res, [
          ['stream_tests2', [ ['333333333-0', [ 'key', 'val2', 'key2', '123' ] ]]],
          ['stream_tests3', [ ['222222222-0', [ 'key', 'val3' ] ]]]
        ]);
      } catch (e) {
        throw e;
      } finally {
        await conn.del('stream_tests2');
        await conn.del('stream_tests3');
      }
    });
  });

  describe('XINFO', () => {
    it('will throw a \'no such key\' error, for all info types if the specified key does not exist.', async () => {
      await asyncThrows(() => conn.xinfo('stream', 'not-exists'),
          /ERR no such key/);
      await asyncThrows(() => conn.xinfo('groups', 'not-exists'),
          /ERR no such key/);
      await asyncThrows(() => conn.xinfo('consumers', 'not-exists', 'group'),
          /ERR no such key/);
    });

    it('will throw an error if attempting to make a \'consumers\' request without a group name.', async () => {
      // todo any way to make this check static typed?
      await asyncThrows(() => conn.xinfo('consumers', 'not-exists'),
          /Group name is required/);
    });

  });
});
