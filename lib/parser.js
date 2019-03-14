const { Writable } = require('stream');
const { MultiBuffer } = require('./util');



/**
 * @constructor
 * @extends {Array<RedisType>}
 */
class RedisArray extends Array {}


/**
 * @typedef {null|string|number|RedisArray|Buffer} RedisType
 */


class RESPParser extends Writable {
  constructor() {
    super({ decodeStrings: false });

    /**
     * @type {MultiBuffer}
     * @private
     */
    this._readBuffer = new MultiBuffer();
    this._remainingBulkStringBytes = -1;
    this._tokenResolver = null;

    /**
     * @type {Array.<(string|Buffer)?>}
     * @private
     */
    this._pendingTokens = [];
  }

  dispose() {
    this._readBuffer = new MultiBuffer();
    this._remainingBulkStringBytes = -1;
    this._tokenResolver = null;
    this._pendingTokens = [];
  }

  _getNextToken() {
    if (this._pendingTokens.length > 0) {
      return this._pendingTokens.shift();
    } else {
      if (this._tokenResolver) throw new Error('already waiting for token');
      return new Promise((resolve /*, reject */) => {
        this._tokenResolver = resolve;
      });
    }
  }

  /**
   * @param {(string|Buffer)?} token The token to push.
   */
  _pushToken(token) {
    if (this._tokenResolver) {
      this._tokenResolver(token);
      this._tokenResolver = null;
    } else {
      this._pendingTokens.push(token);
    }
  }

  _processReadBuffers() {
    if (this._remainingBulkStringBytes > 0) {
      if (this._readBuffer.length >= this._remainingBulkStringBytes + 2) {
        this._pushToken(this._readBuffer.shift(this._remainingBulkStringBytes));
        this._readBuffer.shift(2);
        this._remainingBulkStringBytes = -1;
      } else {
        return;
      }
    }

    while (this._readBuffer.length > 0) {
      const terminal = this._readBuffer.indexOf('\r\n');
      if (terminal === -1) break;

      const msg = this._readBuffer.shift(terminal);
      this._readBuffer.shift(2);  // shift off the \r\n


      if (msg[0] === 36) {  // starting new bulk string
        const len = parseInt(msg.slice(1).toString(), 10);
        if (len === -1) {
          this._pushToken(null);  // null string
        } else if (len === 0) {
          this._pushToken('');    // empty string 
        } else {
          if ((len + 2) <= this._readBuffer.length) {
            this._pushToken(this._readBuffer.shift(len));
            this._readBuffer.shift(2);  // get rid of the \r\n
          } else {
            this._remainingBulkStringBytes = len;
          }
        }
      } else {
        this._pushToken(msg.toString());
      }
    }
  }

  /** 
   * @param {any} chunk
   * @param {!string} encoding
   * @param {() => void} callback
   */
  _write(chunk, encoding, callback) {
    this._readBuffer.add(chunk);
    this._processReadBuffers();
    callback();
  }

  /**
   * [getResponse description]
   * @return {Promise.<RedisType|Error>} The next response.
   */
  async getResponse() {
    const token = await this._getNextToken();

    if (token === null) {
      return token;
    } else if (Buffer.isBuffer(token)) {
      return token.toString('utf-8');      
    } else if (token[0] === '*') {
      const arrlen = parseInt(token.slice(1), 10);
      if (arrlen === -1) return null;  // null array
      /**
       * @type {Array<RedisType>}
       */
      const res = [];
      for (let i = 0; i < arrlen; i++) {
        const el = await this.getResponse();
        if (el instanceof Error) return el;
        res.push(el);
      }
      return res;
    } else if (token[0] === ':') {
      return parseInt(token.slice(1), 10);
    } else if (token[0] === '+') {
      return token.slice(1);      
    } else if (token[0] === '-') {
      return new Error(token.slice(1));
    } else {  // what is it?
      throw new Error(`Unknown token: '${token}'.`);
    }
  }
}

module.exports = { RESPParser };
