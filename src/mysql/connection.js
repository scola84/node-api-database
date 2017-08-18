import sprintf from 'sprintf';
import MysqlQuery from './query';

export default class MysqlConnection {
  constructor() {
    this._config = null;
    this._database = null;
    this._host = null;
    this._index = 0;
    this._mysql = null;
    this._pools = {};
    this._replication = null;
    this._shards = null;
  }

  destroy() {
    Object.keys(this._pools).forEach((name) => {
      this._pools[name].end();
    });
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
    const args = [];

    if (shard !== null && this._shards > 0) {
      args.push(Math.floor(shard / this._shards));
    }

    if (this._replication === true) {
      args.push(this._index);
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

  mysql(value = null) {
    if (value === null) {
      return this._mysql;
    }

    this._mysql = value;
    return this;
  }

  query(query, values = null, callback = null) {
    const instance = new MysqlQuery()
      .connection(this)
      .database(this._database)
      .query(query);

    if (values === null) {
      return instance;
    }

    return instance.execute(values, callback);
  }

  switch () {
    if (this._replication === true) {
      this._index ^= 1;
    }
  }
}
