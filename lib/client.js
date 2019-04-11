const net = require('net');
const parser = require('./parser');



/**
 * @typedef {[parser.RedisType[], boolean, (r: any) => void, (e: Error) => void]}
 * QueuedCommand
 */

/**
 * @typedef {{
 *   ex?: number,
 *   px?: number,
 *   nx?: number,
 *   xx?: number
 * }}
 * SetOptions
 */

/**
 * @typedef {{
 *   [key: string]: string
 * }}
 * StreamEntry
 */

/**
 * @typedef {{
 *   [key: string]: string|'$'
 * }}
 * XReadStreamMap
 */


/**
 * @typedef {{
 *   id?: string,
 *   maxlen?: number,
 *   maxlen_exact?: number
 * }}
 * XAddOptions
 */

/**
 * @typedef {{
 *   count?: number,
 *   block?: number
 * }}
 * XReadOptions
 */

/**
 * @typedef {'stream'|'groups'|'consumers'} StreamInfoType
 */

/**
 * @typedef {'create'|'setid'|'destroy'|'delconsumer'} XGroupType
 */

/**
 * Redis client implementation
 */
class RedisClient {

  /**
   * @param  {net.Socket} socket The socket to use.
   */
  constructor(socket) {
    this._socket = socket;
    this._parser = new parser.RESPParser();
    this._socket.pipe(this._parser);

    /**
     * @type {QueuedCommand[]}
     * @private
     */
    this._commandQueue = [];
    this._running = false;
  }

  /**
   * Close the connection.
   */
  close() {
    this._socket.unpipe(this._parser);
    this._socket.end();
    // this._socket = null;
    this._parser.dispose();
  }


  /**
   * Write a RESP data type to a socket. Doing it this way allows buffers to be
   * written through without their memory being cloned.
   * @param {parser.RedisType} type The thing to encode.
   */
  _write_to_socket(type) {
    if (Array.isArray(type)) {
      this._socket.write(`*${type.length}\r\n`);
      for (let el of type) {
        this._write_to_socket(el);
      }
    } else if (typeof type === 'string' || Buffer.isBuffer(type)) {
      // todo is there a quicker way to check  if a string has multibyte
      // characters?
      // `Buffer.byteLength` is better than constructing a buffer then sending it
      // but node calls it again inside of the `write` so it's kind of a waste
      const len = Buffer.byteLength(type, 'utf8')
      this._socket.write(`$${len}\r\n`);
      this._socket.write(type);
      this._socket.write('\r\n');
    } else if (typeof type === 'number') {
      this._socket.write(`:${type}\r\n`);
    }
  }

  /**
   * Serially run the current 'command queue'.
   * @private
   */
  async _run_command_queue() {
    if (this._running) return;

    this._running = true;
    while (this._commandQueue.length > 0) {
      const [cmd, inline, resolve, reject] = /** @type {!QueuedCommand} */ (
          this._commandQueue.shift());

      if (inline) {
        this._socket.write(cmd.join(' ') + '\r\n');
      } else {
        // cork the socket so that calls to `write` will only be flushed once
        // the command is fully written
        this._socket.cork();
        this._write_to_socket(cmd);
        this._socket.uncork();
      }

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
   * @template T
   * @param {boolean} inline Whether the command can be inlined, setting this to
   *     true can improve performance but the command must not be too large.
   * @param {...parser.RedisType} cmd The command array.
   * @return {!Promise.<T>} A promise to the result.
   */
  _serialize_command(inline, ...cmd) {
    return new Promise((resolve, reject) => {
      this._commandQueue.push([cmd, inline, resolve, reject]);
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
  info(section='default') {
    return this._serialize_command(true, 'INFO', section);
  }

  /**
   * Removes the specified keys. A key is ignored if it does not exist.
   * @param {...string} keys The keys to delete.
   * @return {!Promise.<!number>} The number of keys that were removed.
   */
  del(...keys) {
    return this._serialize_command(true, 'DEL', ...keys);
  }

  /**
   * Get the value of key. If the key does not exist the special value null is
   * returned. An error is returned if the value stored at key is not a string,
   * because GET only handles string values.
   * @param {!string} key The key to get.
   * @return {!Promise.<string|null>} The value of key, or null when key does
   *     not exist.
   */
  get(key) {
    return this._serialize_command(true, 'GET', key);
  }

  /**
   * Set key to hold the string value. If key already holds a value, it is
   * overwritten, regardless of its type. Any previous time to live associated
   * with the key is discarded on successful SET operation.
   * @param {!string} key The key to set.
   * @param {!(string|Buffer)} val The value to set.
   * @param {SetOptions?} options Optional, extra options available since 2.6.
   * @return {!Promise.<string|null>} OK if SET was executed correctly,
   *     or null if the SET operation was not performed because the user
   *     specified the NX or XX option but the condition was not met.
   */
  set(key, val, options=null) {
    /** @type {parser.RedisType} */
    const cmd = ['SET', key, val];
    if (options !== null) {
      if ('ex' in options || 'px' in options) {
        cmd.push(/** @type {number} */ (
            'ex' in options ? options.ex : options.px));
      }
      if (options.nx || options.xx) {
        cmd.push(options.nx ? 'NX' : 'XX');
      }
    }

    return this._serialize_command(val.length < 1000, ...cmd);
  }

  /**
   * Removes the specified fields from the hash stored at key. Specified fields
   * that do not exist within this hash are ignored. If key does not exist, it
   * is treated as an empty hash and this command returns 0.
   * @param {!string} key The key of the hash.
   * @param {!string} field The field name to remove from the hash.
   * @param {...string} fields Optionally, more field names to remove.
   * @return {!Promise.<!number>} The number of fields that were removed from the
   *     hash, not including specified but non existing fields.
   */
  hdel(key, field, ...fields) {
    return this._serialize_command(true, 'HDEL', key, field, ...fields);
  }

  /**
   * Returns if field is an existing field in the hash stored at key.
   * @param {!string} key The key of the hash.
   * @param {!string} field The field name to check.
   * @return {!Promise.<!number>} 1 if the hash contains field or 0 if the hash
   *     does not contain field, or key does not exist.
   */
  hexists(key, field) {
    return this._serialize_command(true, 'HEXISTS', key, field);
  }

  /**
   * Returns the value associated with field in the hash stored at key.
   * @param {!string} key The key of the hash.
   * @param {!string} field The field.
   * @return {!Promise.<?string>} The value associated with field, or null when
   *     field is not present in the hash or key does not exist.
   */
  hget(key, field) {
    return this._serialize_command(true, 'HGET', key, field);
  }

  /**
   * Returns all fields and values of the hash stored at key. In the returned
   * value, every field name is followed by its value, so the length of the
   * reply is twice the size of the hash.
   * @param {!string} key The key of the hash.
   * @return {!Promise.<!string[]>} Alternating list of field name, and it's
   *     respective value.
   */
  hgetall(key) {
    return this._serialize_command(true, 'HGETALL', key);
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
   * @return {!Promise.<!number>} The value at field after the increment
   *     operation.
   */
  hincrby(key, field, increment) {
    return this._serialize_command(true, 'HINCRBY', key, field, String(increment));
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
   * @return {!Promise.<!string>} The value of field after the increment.
   */
  hincrbyfloat(key, field, increment) {
    return this._serialize_command(true, 'HINCRBYFLOAT', key, field, String(increment));
  }

  /**
   * Returns all field names in the hash stored at key.
   * @param {!string} key The key of the hash.
   * @return {Promise.<!string[]>} list of fields in the hash, or an empty list
   *     when key does not exist.
   */
  hkeys(key) {
    return this._serialize_command(true, 'HKEYS', key);
  }

  /**
   * Returns the number of fields contained in the hash stored at key.
   * @param {!string} key The key of the hash.
   * @return {!Promise.<!number>} Number of fields in the hash, or 0 when key
   *     does not exist.
   */
  hlen(key) {
    return this._serialize_command(true, 'HLEN', key);
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
   * @return {!Promise.<?string>} list of values associated with the given
   *     fields, in the same order as they are requested.
   */
  hmget(key, field, ...fields) {
    return this._serialize_command(true, 'HMGET', key, field, ...fields);
  }

  /**
   * Sets the specified fields to their respective values in the hash stored at
   * key. This command overwrites any specified fields already existing in the
   * hash. If key does not exist, a new key holding a hash is created.
   * @param {!string} key The key of the hash.
   * @param {!string} field The first field name to set.
   * @param {!string} value The first field value to set..
   * @param {...string} fieldvals Optionally, more field, value pairs to set.
   * @return {!Promise.<!string>} Will return "OK" if successful.
   */
  hmset(key, field, value, ...fieldvals) {
    return this._serialize_command(true, 'HMSET', key, field, value, ...fieldvals);
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
   * @return {!Promise.<!number>} Either 1 or 0: 1 if field is a new field in the
   *     hash, or 0 if the field already existed and was updated.
   */
  hset(key, field, value) {
    return this._serialize_command(value.length < 1000, 'HSET', key, field, String(value));
  }

  /**
   * Sets field in the hash stored at key to value, only if field does not yet
   * exist. If key does not exist, a new key holding a hash is created. If field
   * already exists, this operation has no effect.
   * @param {!string} key The key of the hash.
   * @param {!string} field The field name of the hash.
   * @param {*} value The value to store.
   * @return {!Promise.<!number>} Either 1 or 0: 1 if field is a new field in the
   *     hash, or 0 if the field already existed and no operation was performed.
   */
  hsetnx(key, field, value) {
    return this._serialize_command(value.length < 1000, 'HSETNX', key, field, value);
  }

  /**
   * Returns the string length of the value associated with field in the hash
   * stored at key. If the key or the field do not exist, 0 is returned.
   * @param {!string} key The key of the hash.
   * @param {!string} field The field name of the hash.
   * @return {!Promise.<!number>} The string length of the value associated with
   *     field, or zero when field is not present in the hash or key does not
   *     exist at all.
   */
  hstrlen(key, field) {
    return this._serialize_command(true, 'HSTRLEN', key, field);
  }

  /**
   * Returns all values in the hash stored at key.
   * @param {!string} key The key of the hash.
   * @return {!Promise.<!string[]>} List of values in the hash, or an empty list
   * when key does not exist.
   */
  hvals(key) {
    return this._serialize_command(true, 'HVALS', key);
  }

  /**
   * Appends the specified stream entry to the stream at the specified key. If
   * the key does not exist, as a side effect of running this command the key is
   * created with a stream value.
   * @param {!string} key The key of the stream.
   * @param {StreamEntry} entry
   * @param {XAddOptions=} options Optional, options to the xadd request.
   * @return {!Promise.<!string>} The ID of the added entry.
   */
  xadd(key, entry, options={}) {
    const cmd = ['XADD', key];
    if (options.maxlen_exact) {
      cmd.push('MAXLEN', String(options.maxlen_exact));
    } else if (options.maxlen) {
      cmd.push('MAXLEN', '~', String(options.maxlen));
    }
    cmd.push(options.id || '*');

    // flatten the stream entry message to [field, val, field, val, ...]
    const with_fieldvals = cmd.concat(...Object.entries(entry));
    return this._serialize_command(false, ...with_fieldvals);
  }

  /**
   * This command is used in order to manage the consumer groups associated with
   * a stream data structure. Using XGROUP you can:
   *  - Create a new consumer group associated with a stream.
   *  - Destroy a consumer group.
   *  - Remove a specific consumer from a consumer group.
   *  - Set the consumer group last delivered ID to something else.
   * @param {!XGroupType} type The type of the xgroup request.
   * @param {!string} key The key of the stream.
   * @param {!string} groupname The group name.
   * @param {(number|'$'|string)=} id_or_consumername The id of the last entry in
   *    the stream to be considered delivered, or the special id '$' (that
   *    means: the ID of the last item in the stream) for the or in the case of
   *    'delconsumer' the consumer name to delete.
   * @return {!Promise.<!'OK'>} 
   */
  xgroup(type, key, groupname, id_or_consumername=undefined) {
    const upperType = type.toUpperCase();
    const cmd = ['XGROUP', type, key, groupname];
    if (type !== 'destroy') {
      if (id_or_consumername === undefined) {
        if (type === 'delconsumer') {
          throw new Error('A consumer name must be provided for a DELCONSUMER request.');
        } else {
          throw new Error(`The id of the last consumed entry, or '$' must be specified for a ${upperType} request.`);
        }
      } else {
        cmd.push(String(id_or_consumername));
      }
    }

    return this._serialize_command(true, ...cmd);
  }

  /**
   * Read data from one or multiple streams, only returning entries with an ID
   * greater than the last received ID reported by the caller. This command has
   * an option to block if items are not available, in a similar fashion to
   * BRPOP or BZPOPMIN and others.
   * @param {!XReadStreamMap} streams A map of requested the streams. Format is
   *     { 'stream name': last id, ... }.
   * @param {XReadOptions=} options Various options for the xread command. 
   */
  xread(streams, options={}) {
    let cmd = ['XREAD'];
    if ('count' in options) {
      cmd.push('COUNT', String(options.count));
    }
    if ('block' in options) {
      cmd.push('BLOCK', String(options.block));
    }
    cmd.push('STREAMS');
    const ids = [];
    for (let name in streams) {
      cmd.push(name);
      ids.push(streams[name]);
    }
    cmd = cmd.concat(ids);

    return this._serialize_command(false, ...cmd);
  }

  /**
   * This is an introspection command used in order to retrieve different
   * information about the streams and associated consumer groups.
   * For more information see: https://redis.io/commands/xinfo
   * @param {!StreamInfoType} type The type of information requested.
   * @param {!string} key The key of the stream.
   * @param {string=} group_name If type='consumers' the consumer group name.
   * @return {!Promise.<!parser.RedisType>} The requested info.
   */
  xinfo(type, key, group_name=undefined) {
    const cmd = ['XINFO', type.toUpperCase(), key];
    if (type === 'consumers') {
      if (!group_name) throw new Error(
          'Group name is required for an `XINFO CONSUMERS` request.');
      cmd.push(group_name);
    }

    return this._serialize_command(true, ...cmd);
  }
}



module.exports = RedisClient;
