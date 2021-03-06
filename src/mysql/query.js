import sprintf from 'sprintf';

export default class MysqlQuery {
  constructor() {
    this._cache = null;
    this._connection = null;
    this._database = null;
    this._prefix = null;
    this._query = null;
    this._replication = null;
    this._shard = null;
  }

  cache(value = null) {
    if (value === null) {
      return this._cache;
    }

    this._cache = value;
    return this;
  }

  connection(value = null) {
    if (value === null) {
      return this._connection;
    }

    this._connection = value;
    return this;
  }

  database(value = null) {
    if (value === null) {
      return this._database;
    }

    this._database = value;
    return this;
  }

  query(value = null) {
    if (value === null) {
      return this._query;
    }

    this._query = value;
    return this;
  }

  prefix(value = null) {
    if (value === null) {
      return this._prefix;
    }

    this._prefix = value;
    return this;
  }

  replication(value = null) {
    if (value === null) {
      return this._replication;
    }

    this._replication = value;
    return this;
  }

  shard(value = null) {
    if (value === null) {
      return this._shard;
    }

    this._shard = Number(value);
    return this;
  }

  execute(values, callback = null, retry = null) {
    if (typeof values === 'function') {
      retry = callback;
      callback = values;
      values = null;
    }

    const query = sprintf(this._query, {
      db: sprintf(this._database, this._shard)
    });

    if (this._prefix === null) {
      this._execute(query, values, callback, retry);
      return;
    }

    this._cache.get(this._prefix, this._field(query, values),
      (error, data) => {
        if (error) {
          callback(error);
          return;
        }

        if (typeof data !== 'undefined') {
          this._finish(null, data, callback);
          return;
        }

        this._execute(query, values, callback, retry);
      });
  }

  _execute(query, values, callback, retry) {
    this
      ._connection
      .get(this._shard)
      .query(query, values, (error, result) => {
        retry = retry !== false &&
          this._replication === true &&
          this._error(error) === true;

        if (retry === true) {
          this._connection.index(this._shard, true);
          this.execute(values, callback, false);
          return;
        }

        if (error || this._prefix === null) {
          this._finish(error, result, callback);
          return;
        }

        this._cache.set(this._prefix, this._field(query, values), result,
          (cacheError, cacheData) => {
            this._finish(cacheError, cacheData, callback);
          });
      });
  }

  _finish(error, data, callback) {
    if (callback !== null) {
      callback(error, data);
    }
  }

  _field(query, values = []) {
    return query + String(values);
  }

  _error(error) {
    return error instanceof Error === true &&
      (error.code === 'PROTOCOL_CONNECTION_LOST' ||
        error.code === 'ECONNREFUSED' ||
        error.code === 'ER_SERVER_SHUTDOWN');
  }
}
