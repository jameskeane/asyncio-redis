class MultiBuffer {
  /**
   * @param {Buffer[]} buffers The buffers.
   * @param {number?} bytes The total length of all buffers.
   */
  constructor(buffers=[], bytes=null) {
    this._buffers = buffers;
    this._length = bytes || buffers.reduce((acc, buf) => acc + buf.length, 0);
  }

  get length() { return this._length; }

  /**
   * Add a buffer to the end of the multi.
   * @param {Buffer} buffer The buffer to add
   */
  add(buffer) {
    this._buffers.push(buffer);
    this._length += buffer.length;  
  }

  /**
   * Shift n bytes off the front of the buffer
   * @param  {!number} numBytes The number of bytes to shift off.
   * @return {Buffer} The first n bytes
   */
  shift(numBytes) {
    if (numBytes > this._length) throw new Error('Can\'t shift bytes, not enough.');

    const bufs = [];
    let lenacc = 0;
    while(lenacc < numBytes) {
      const buf = /** @type {Buffer} */ (this._buffers.shift());
      const remaining = numBytes - lenacc;

      if (buf.length > remaining) {
        bufs.push(buf.slice(0, remaining));
        this._buffers.unshift(buf.slice(remaining));
        this._length -= remaining;
        lenacc += remaining;
      } else {
        bufs.push(buf);
        this._length -= buf.length;
        lenacc += buf.length;
      }
    }

    return bufs.length === 1 ? bufs[0] : Buffer.concat(bufs);
  }

  /**
   * Return the index into this multibuffer of the given character.
   * @param {!string} char The character or string to search for.
   * @return {number} The index, or -1 if not found.
   */
  indexOf(char) {
    let offset = 0;
    for (let buf of this._buffers) {
      const idx = buf.indexOf(char);
      if (idx !== -1) return offset + idx;
      offset += buf.length;
    }
    return -1;
  }

  /**
   * Stringify the entire buffer
   * @param {string} encoding The encoding to use.
   * @return {!string} the decoded string.
   */
  toString(encoding='utf-8') {
    return this._buffers.reduce((acc, b) => acc + b.toString(encoding), '');
  }
}

module.exports = { MultiBuffer };
