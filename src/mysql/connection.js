import sprintf from 'sprintf';
import { debuglog } from 'util';
import MysqlQuery from './query';

export default class MysqlConnection {
  constructor() {
    this._log = debuglog('database');

    this._cache = null;
    this._config = null;
    this._database = null;
    this._host = null;
    this._indices = {};
    this._mysql = null;
    this._pools = {};
    this._replication = null;
    this._shards = null;
  }

  destroy() {
    this._log('MysqlConnection destroy');

    Object.keys(this._pools).forEach((name) => {
      this._pools[name].end();
    });
  }

  cache(value = null) {
    if (value === null) {
      return this._cache;
    }

    this._cache = value;
    return this;
  }

  config(value) {
    const {
      database,
      host,
      replication = false,
      shards = 0
    } = value;

    this._database = database;
    this._host = host;
    this._replication = replication;
    this._shards = shards;

    this._config = Object.assign({}, value);

    delete this._config.database;
    delete this._config.host;
    delete this._config.replication;
    delete this._config.shards;

    return this;
  }

  get(shard = null) {
    this._log('MysqlConnection get shard=%j host=%s',
      shard, this._host);

    const args = [];

    if (shard !== null && this._shards > 0) {
      args.push(this._number(shard));
    }

    if (this._replication === true) {
      args.push(this.index(shard));
    }

    const host = sprintf(this._host, ...args);

    if (typeof this._pools[host] === 'undefined') {
      this._pools[host] = this._mysql
        .createPool(Object.assign({}, this._config, {
          host
        }));
    }

    return this._pools[host];
  }

  index(shard = null, update = false) {
    let number = 0;

    if (shard !== null) {
      number = this._number(shard);
    }

    if (typeof this._indices[number] === 'undefined') {
      this._indices[number] = 0;
    }

    if (update === true && this._replication === true) {
      this._indices[number] ^= 1;
    }

    this._log('MysqlConnection index shard=%d update=%j index=%d',
      shard, update, this._indices[number]);

    return this._indices[number];
  }

  mysql(value = null) {
    if (value === null) {
      return this._mysql;
    }

    this._mysql = value;
    return this;
  }

  query(query, values = null, callback = null) {
    this._log('MysqlConnection query query=%s values=%j',
      query, values);

    const instance = new MysqlQuery();

    instance.connection(this);
    instance.cache(this._cache);
    instance.database(this._database);
    instance.replication(this._replication);
    instance.query(query);

    if (values === null) {
      return instance;
    }

    return instance.execute(values, callback);
  }

  _number(shard) {
    return Math.floor(shard / this._shards);
  }
}
