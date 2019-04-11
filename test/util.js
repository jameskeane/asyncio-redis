const assert = require('assert');



/**
 * `assert.throws` but for async functions.
 * @param {Function} fn The async function to test.
 * @param {!RegExp} msg The exception message
 */
async function asyncThrows(fn, msg) {
  let f = () => {};
  try {
    await fn();
  } catch (e) {
    f = () => { throw e; }
  } finally {
    assert.throws(f, msg);
  }
}


module.exports = { asyncThrows };
