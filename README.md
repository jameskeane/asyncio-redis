# @asyncio/redis

A high performance async/await native redis client library.

Fully type-checked, fully documented, no compilation needed.

Supports Node 8.X LTS.


## Install

Install with [NPM](https://npmjs.org/):
```
    npm install @asyncio/redis
```

## Usage

```javascript
const redis = require('@asyncio/redis');

async function main() {
  const conn = await reddis.connect();

  await conn.hset('hash', 'field', 'value');

}
main();

```

