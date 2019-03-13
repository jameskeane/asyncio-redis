const net = require('net');
const RedisClient = require('./lib/client');



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

    /**
     * @param  {Error} err The error
     */
    function onceError(err) {
      socket.removeListener('connect', onceConnect);
      reject(err);
    }

    socket.once('connect', onceConnect);
    socket.once('error', onceError);
  });
}



module.exports = { connect, RedisClient };
