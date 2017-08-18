import sprintf from 'sprintf';

export default class MysqlQuery {
  constructor() {
    this._attempt = 0;
    this._connection = null;
    this._database = null;
    this._query = null;
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

  shard(value = null) {
    if (value === null) {
      return this._shard;
    }

    this._shard = value;
    return this;
  }

  execute(values, callback = null) {
    if (typeof values === 'function') {
      callback = values;
      values = null;
    }

    const replace = sprintf(this._database, this._shard);
    const query = sprintf(this._query, {
      database: replace,
      db: replace
    });

    this._attempt += 1;

    this
      ._connection
      .get(this._shard)
      .query(query, values, (error, result) => {
        if (this._error(error)) {
          this._connection.switch();

          if (this._attempt === 1) {
            this.execute(values, callback);
            return;
          }
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
