import sprintf from 'sprintf';

export default class MysqlQuery {
  constructor() {
    this._connection = null;
    this._database = null;
    this._query = null;
    this._replication = null;
    this._shard = null;
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

    this._shard = value;
    return this;
  }

  execute(values, callback = null, retry = null) {
    if (typeof values === 'function') {
      retry = callback;
      callback = values;
      values = null;
    }

    const replace = sprintf(this._database, this._shard);
    const query = sprintf(this._query, {
      database: replace,
      db: replace
    });

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

        if (callback !== null) {
          callback(error, result);
        }
      });
  }

  _error(error) {
    return error instanceof Error === true &&
      (error.code === 'PROTOCOL_CONNECTION_LOST' ||
        error.code === 'ECONNREFUSED' ||
        error.code === 'ER_SERVER_SHUTDOWN');
  }
}
