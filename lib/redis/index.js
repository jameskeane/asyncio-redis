const net = require('net');
const { RESPParser } = require('./parser');



/**
 * @typedef {string|null|number|Array.<RedisType>}
 */
var RedisType;



/**
 * Redis client implementation
 */
class RedisClient {
  constructor(socket) {
    this._socket = socket;
    this._parser = new RESPParser();
    this._socket.pipe(this._parser);
    this._commandQueue = [];
    this._running = false;
  }

  /**
   * Serially run the current 'command queue'.
   * @private
   */
  async _run_command_queue() {
    if (this._running) return;
    this._running = true;
    while (this._commandQueue.length > 0) {
      const [cmd, resolve, reject] = this._commandQueue.shift();
      this._socket.write(cmd);
      const res = await this._parser.getResponse();
      if (res instanceof Error) {
        reject(res);
      } else {
        resolve(res);
      }
    }
    this._running = false;
  }

  /**
   * Helper method to push a command onto the serialization 'command queue'
   * @param {!string} str The command
   * @return {!Promise.<RedisType>} A promise to the result.
   */
  _serialize_command(str) {
    return new Promise((resolve, reject) => {
      this._commandQueue.push([str, resolve, reject]);
      this._run_command_queue();
    });
  }

  /**
   * The INFO command returns information and statistics about the server in a
   * format that is simple to parse by computers and easy to read by humans.
   * The optional parameter can be used to select a specific section of
   * information, for more details see: https://redis.io/commands/info
   * @param {string?} section Optional, desired section of information.
   * @return {!Promise.<string>} The info, lines can contain a section name
   *     (starting with a # character) or a property. All the properties are in
   *     the form of field:value terminated by \r\n.
   */
  info() {
    return this._serialize_command(`INFO\r\n`);
  }

  /**
   * Removes the specified keys. A key is ignored if it does not exist.
   * @param {...string} keys The keys to delete.
   * @return {!Promise.<number>} The number of keys that were removed.
   */
  del(...keys) {
    return this._serialize_command(`DEL ${keys.join(' ')}\r\n`);
  }

  /**
   * Removes the specified fields from the hash stored at key. Specified fields
   * that do not exist within this hash are ignored. If key does not exist, it
   * is treated as an empty hash and this command returns 0.
   * @param {!string} key The key of the hash.
   * @param {!string} field The field name to remove from the hash.
   * @param {...string} fields Optionally, more field names to remove.
   * @return {!Promise.<number>} The number of fields that were removed from the
   *     hash, not including specified but non existing fields.
   */
  hdel(key, field, ...fields) {
    const fields_str = [field].concat(fields).join(' ');
    return this._serialize_command(`HDEL ${key} ${fields_str}\r\n`);
  }

  /**
   * Returns if field is an existing field in the hash stored at key.
   * @param {!string} key The key of the hash.
   * @param {!string} field The field name to check.
   * @return {!Promise.<number>} 1 if the hash contains field or 0 if the hash
   *     does not contain field, or key does not exist.
   */
  hexists(key, field) {
    return this._serialize_command(`HEXISTS ${key} ${field}\r\n`);
  }

  /**
   * Returns the value associated with field in the hash stored at key.
   * @param {!string} key The key of the hash.
   * @param {!string} field The field.
   * @return {!Promise.<string?>} The value associated with field, or null when
   *     field is not present in the hash or key does not exist.
   */
  hget(key, field) {
    return this._serialize_command(`HGET ${key} ${field}\r\n`);
  }

  /**
   * Returns all fields and values of the hash stored at key. In the returned
   * value, every field name is followed by its value, so the length of the
   * reply is twice the size of the hash.
   * @param {!string} key The key of the hash.
   * @return {!Promise.<string[]>} Alternating list of field name, and it's
   *     respective value.
   */
  hgetall(key) {
    return this._serialize_command(`HGETALL ${key}\r\n`);
  }

  /**
   * Increments the number stored at field in the hash stored at key by
   * increment. If key does not exist, a new key holding a hash is created. If
   * field does not exist the value is set to 0 before the operation is
   * performed.
   * The range of values supported by HINCRBY is limited to 64 bit signed
   * integers.
   * @param {!string} key The key of the hash.
   * @param {!string} field The field to increment
   * @param {!number} increment The amount to increment it by.
   * @return {!Promise.<number>} The value at field after the increment
   *     operation.
   */
  hincrby(key, field, increment) {
    return this._serialize_command(`HINCRBY ${key} ${field} ${increment}\r\n`);
  }

  /**
   * Increment the specified field of a hash stored at key, and representing a
   * floating point number, by the specified increment. If the increment value
   * is negative, the result is to have the hash field value decremented instead
   * of incremented. If the field does not exist, it is set to 0 before
   * performing the operation. An error is returned if one of the following
   * conditions occur:
   *  - The field contains a value of the wrong type (not a string).
   *  - The current field content or the specified increment are not parsable
   *    as a double precision floating point number.
   * The exact behavior of this command is identical to the one of the
   * INCRBYFLOAT command, please refer to the documentation of INCRBYFLOAT for
   * further information.
   * @param {!string} key The key of the hash.
   * @param {!string} field The field to increment
   * @param {!number} increment The amount to increment it by.
   * @return {!Promise.<string>} The value of field after the increment.
   */
  hincrbyfloat(key, field, increment) {
    return this._serialize_command(
        `HINCRBYFLOAT ${key} ${field} ${increment}\r\n`);
  }

  /**
   * Returns all field names in the hash stored at key.
   * @param {!string} key The key of the hash.
   * @return {Promise.<string[]>} list of fields in the hash, or an empty list
   *     when key does not exist.
   */
  hkeys(key) {
    return this._serialize_command(`HKEYS ${key}\r\n`);
  }

  /**
   * Returns the number of fields contained in the hash stored at key.
   * @param {!string} key The key of the hash.
   * @return {!Promise.<number>} Number of fields in the hash, or 0 when key
   *     does not exist.
   */
  hlen(key) {
    return this._serialize_command(`HLEN ${key}\r\n`);
  }

  /**
   * Returns the values associated with the specified fields in the hash stored
   * at key.
   * For every field that does not exist in the hash, a nil value is returned.
   * Because non-existing keys are treated as empty hashes, running HMGET
   * against a non-existing key will return a list of nil values.
   * @param {!string} key The key of the hash.
   * @param {!string} field The field name to resolve from the hash.
   * @param {...string} fields Optionally, more field names to resolve.
   * @return {!Promise.<string|null>} list of values associated with the given
   *     fields, in the same order as they are requested.
   */
  hmget(key, field, ...fields) {
    const fields_str = [field].concat(fields).join(' ');
    return this._serialize_command(`HMGET ${key} ${fields_str}\r\n`);
  }

  /**
   * Sets the specified fields to their respective values in the hash stored at
   * key. This command overwrites any specified fields already existing in the
   * hash. If key does not exist, a new key holding a hash is created.
   * @param {!string} key The key of the hash.
   * @param {!string} field The first field name to set.
   * @param {!string} value The first field value to set..
   * @param {...string} fieldvals Optionally, more field, value pairs to set.
   * @return {!Promise.<string>} Will return "OK" if successful.
   */
  hmset(key, field, value, ...fieldvals) {
    const fieldvals_str = [field, value].concat(fieldvals).join(' ');
    return this._serialize_command(`HMSET ${key} ${fieldvals_str}\r\n`);
  }

  /**
   * See `scan` for `hscan` documentation.
   * @todo
   */
  hscan() {
    // todo
  }

  /**
   * Sets field in the hash stored at key to value. If key does not exist,
   * a new key holding a hash is created. If field already exists in the hash,
   * it is overwritten.
   * @param {!string} key The key of the hash.
   * @param {!string} field The field name of the hash.
   * @param {*} value The value to store.
   * @return {!Promise.<number>} Either 1 or 0: 1 if field is a new field in the
   *     hash, or 0 if the field already existed and was updated.
   */
  hset(key, field, value) {
    return this._serialize_command(`HSET ${key} ${field} ${value}\r\n`);
  }

  /**
   * Sets field in the hash stored at key to value, only if field does not yet
   * exist. If key does not exist, a new key holding a hash is created. If field
   * already exists, this operation has no effect.
   * @param {!string} key The key of the hash.
   * @param {!string} field The field name of the hash.
   * @param {*} value The value to store.
   * @return {!Promise.<number>} Either 1 or 0: 1 if field is a new field in the
   *     hash, or 0 if the field already existed and no operation was performed.
   */
  hsetnx(key, field, value) {
    return this._serialize_command(`HSETNX ${key} ${field} ${value}\r\n`);
  }

  /**
   * Returns the string length of the value associated with field in the hash
   * stored at key. If the key or the field do not exist, 0 is returned.
   * @param {!string} key The key of the hash.
   * @param {!string} field The field name of the hash.
   * @return {!Promise.<number>} The string length of the value associated with
   *     field, or zero when field is not present in the hash or key does not
   *     exist at all.
   */
  hstrlen(key, field) {
    return this._serialize_command(`HSTRLEN ${key} ${field}\r\n`);
  }

  /**
   * Returns all values in the hash stored at key.
   * @param {!string} key The key of the hash.
   * @return {!Promise.<string[]>} List of values in the hash, or an empty list
   * when key does not exist.
   */
  hvals(key) {
    return this._serialize_command(`HVALS ${key}\r\n`);
  }

  close() {
    this._socket.unpipe(this._parser);
    this._socket.end();
    this._socket = null;
    this._parser.dispose();
  }
}



/**
 * Create a new connection.
 * @return {Promise.<RedisClient>} A promise to the client. 
 */
function connect() {
  return new Promise((resolve, reject) => {
    const socket = net.createConnection({ port: 6379 });

    function onceConnect() {
      socket.removeListener('error', onceError);
      resolve(new RedisClient(socket));
    }

    function onceError(err) {
      socket.removeListener('connect', onceConnect);
      reject(err);
    }

    socket.once('connect', onceConnect);
    socket.once('error', onceError);
  });
}



module.exports = { connect, RedisClient };
