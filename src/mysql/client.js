import { debuglog } from 'util';
import MysqlConnection from './connection';

export default class MysqlClient {
  constructor() {
    this._log = debuglog('database');

    this._cache = null;
    this._config = {};
    this._connections = {};
    this._mysql = null;
  }

  destroy() {
    this._log('MysqlClient destroy');

    Object.keys(this._connections).forEach((name) => {
      this._connections[name].destroy();
    });

    this._connections = {};
  }

  cache(value = null) {
    if (value === null) {
      return this._cache;
    }

    this._cache = value;
    return this;
  }

  config(value = null) {
    if (value === null) {
      return this._config;
    }

    this._config = value;
    return this;
  }

  connection(name = 'default') {
    this._log('MysqlClient connection name=%s', name);

    if (typeof this._connections[name] === 'undefined') {
      const connection = new MysqlConnection();
      const config = this._config[name] ||
        this._config.default;

      connection.mysql(this._mysql);
      connection.cache(this._cache);
      connection.config(config);

      this._connections[name] = connection;
    }

    return this._connections[name];
  }

  mysql(value = null) {
    if (value === null) {
      return this._mysql;
    }

    this._mysql = value;
    return this;
  }

  query(name, query, values, callback) {
    return this
      .connection(name)
      .query(query, values, callback);
  }
}
