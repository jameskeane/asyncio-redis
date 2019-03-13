const net = require('net');
const { RESPParser } = require('./parser');



class RedisClient {
  constructor(socket) {
    this._socket = socket;
    this._parser = new RESPParser();
    this._socket.pipe(this._parser);
  }

  info() {
    this._socket.write(`INFO\r\n`);
    return this._parser.getResponse();
  }

  /**
   * Sets field in the hash stored at key to value. If key does not exist,
   * a new key holding a hash is created. If field already exists in the hash,
   * it is overwritten.
   * @param {!string} key The key of the hash.
   * @param {!string} field The field name of the hash.
   * @param {*} value The value to store.
   * @return {!number} Either 1 or 0: 1 if field is a new field in the hash, or
   *     0 if the field already existed and was updated.
   */
  hset(key, field, value) {
    this._socket.write(`HSET ${key} ${field} ${value}\r\n`);
    return this._parser.getResponse();
  }

  /**
   * Returns the value associated with field in the hash stored at key.
   * @param {!string} key The key of the hash.
   * @param {!string} field The field.
   * @return {string?} The value associated with field, or null when field is
   *     not present in the hash or key does not exist.
   */
  hget(key, field) {
    this._socket.write(`HGET ${key} ${field}\r\n`);
    return this._parser.getResponse();
  }

  /**
   * Returns all field names in the hash stored at key.
   * @param {!string} key The key of the hash.
   * @return {string[]} list of fields in the hash, or an empty list when key
   *     does not exist.
   */
  hkeys(key) {
    this._socket.write(`HKEYS ${key}\r\n`);
    return this._parser.getResponse();
  }

  close() {

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
