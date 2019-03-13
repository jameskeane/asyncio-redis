const { Writable } = require('stream');



class RESPParser extends Writable {
  constructor() {
    super({ decodeStrings: false });
    this._readBuffers = [];
    this._remainingBulkStringBytes = -1;
    this._tokenResolver = null;
    this._pendingTokens = [];
  }

  dispose() {
    this._readBuffers = null;
    this._tokenResolver = null;
    this._pendingTokens = null;
  }

  _getNextToken() {
    if (this._pendingTokens.length > 0) {
      return Promise.resolve(this._pendingTokens.shift());
    } else {
      if (this._tokenResolver) throw new Error('already waiting for token');
      return new Promise((resolve, reject) => {
        this._tokenResolver = resolve;
      });
    }
  }

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
      const concats = [];
      let remaining = this._remainingBulkStringBytes;
      for (let i = 0; i < this._readBuffers.length; i++) {
        const buf = this._readBuffers[i];
        if (buf.length > remaining) {
          concats.push(buf.slice(0, remaining));
          this._readBuffers = [buf.slice(remaining)].concat(
              this._readBuffers.slice(i + 1));
          remaining = 0;
          break;
        } else {
          concats.push(buf);
          remaining -= buf.length;
        }
      }

      if (remaining != 0) return; // not enough data
      const bs = Buffer.concat(concats).toString('utf-8');
      this._pushToken('+' + bs);
      this._remainingBulkStringBytes = -1;
    } else {
      // pull out the first token delimited by '\r\n'
      let termBufIdx = -1, terminal = -1;
      for (let i = 0; i < this._readBuffers.length; i++) {
        const buf = this._readBuffers[i];
        terminal = buf.indexOf('\r\n');
        if (terminal !== -1) {
          termBufIdx = i;
          break;
        }
      }
      if (termBufIdx === -1) return;

      // If we found a terminal (i.e. '\r\n') we need to join the message and
      // reset the remaining read buffers
      const part = this._readBuffers[termBufIdx].slice(0, terminal);
      const remaining = this._readBuffers[termBufIdx].slice(terminal + 2);
      const msg = Buffer.concat(this._readBuffers.slice(0, termBufIdx).concat(part));
      this._readBuffers = [remaining].concat(this._readBuffers.slice(termBufIdx + 1));

      if (msg.length === 0) {
        // happens after parsing a bulk string: do nothing
      }
      else if (String.fromCharCode(msg[0]) === '$') {  // starting new bulk string
        const len = parseInt(msg.slice(1), 10);
        if (len === -1) {
          this._pushToken(null);  // null string
        } else if (len === 0) {
          this._pushToken('');    // empty string 
        } else {
          this._remainingBulkStringBytes = len;
        }
      } else {
        this._pushToken(msg.toString());
      }
    }
    this._processReadBuffers();
  }

  _write(chunk, encoding, callback) {
    this._readBuffers.push(chunk);
    this._processReadBuffers();
    callback();
  }

  async getResponse() {
    const token = await this._getNextToken();
    if (token === null) {
      return token;
    } else if (token[0] === '*') {
      const arrlen = parseInt(token.slice(1), 10);
      if (arrlen === -1) return null;  // null array
      const res = [];
      for (let i = 0; i < arrlen; i++) {
        res.push(await this.getResponse());
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
