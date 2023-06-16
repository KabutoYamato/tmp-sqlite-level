import { AbstractBatchOperation, AbstractBatchOptions, AbstractClearOptions, AbstractDatabaseOptions, AbstractDelOptions, AbstractGetManyOptions, AbstractGetOptions, AbstractIteratorOptions, AbstractKeyIteratorOptions, AbstractLevel, AbstractOpenOptions, AbstractPutOptions, AbstractSublevelOptions, AbstractValueIteratorOptions, NodeCallback } from "abstract-level";
import { SQLD, SQLiteLevelSQL, SQLiteLevelSchema } from "./sql";
import { SQLiteIterator, SQLiteKeyIterator, SQLiteValueIterator, SomeSQLiteIterator } from "./iterator";
import { ModuleError } from "../core/error";
import { SQLiteSublevel } from "./sublevel";
import { SQLiteChainedBatch } from "./chained-batch";

export class SQLiteLevel<
  SQLK = string,
  SQLV = object
> extends AbstractLevel<SQLD, SQLK, SQLV> {
  conn: SQLiteLevelSQL<SQLK, SQLV>;
  idx = 0;
  itcn: Map<number, SomeSQLiteIterator<SQLK, SQLV>>;

  constructor(
    location: string,
    options: AbstractDatabaseOptions<SQLK, SQLV> & SQLiteLevelSchema = {}
  ) {
    super(
      {
        clear: true, // true
        snapshots: false,
        streams: false,
        encodings: { utf8: true},
        seek: false,
        createIfMissing: false,
        errorIfExists: false,
        getMany: true,
        keyIterator: true, // true
        valueIterator: true, // true
        iteratorNextv: true, // true
        iteratorAll: true, // true
        permanence: true,
        deferredOpen: true, // true
        promises: true,
        status: true,
        events: {},
        additionalMethods: {},
      },
      options
    );

    this.conn = new SQLiteLevelSQL(location, {
      levelTable: options.levelTable,
      idxName: options.idxName,
    });

    this.itcn = new Map();
  }

  get db(){
    return this;
  }

  get prefix() {
    return undefined;
  }

  // INSTANCE MANAGEMENT

  async _open(options: AbstractOpenOptions, callback: NodeCallback<any>) {
    await this.conn.OpenConnection(options);
    return this.nextTick(callback);
  }

  async _close(callback: NodeCallback<any>) {
    await this.conn.CloseConnection();
    this.itcn.forEach((it) => {
      it.close();
    });
    return this.nextTick(callback);
  }

  // CRUD OPERATIONS

  async _get(
    key: SQLK,
    options: AbstractGetOptions<SQLK, SQLV>,
    callback: NodeCallback<any>
  ): Promise<void> {
    const val = await this.conn.GetValByKey(this, key, options);
    if (val === undefined) {
      return this.nextTick(
        callback,
        new ModuleError(`key was not found`, {
          code: "LEVEL_NOT_FOUND",
        })
      );
    }
    return this.nextTick(callback, null, val);
  }

  async _getMany(
    keys: SQLD[],
    options: AbstractGetManyOptions<SQLK, SQLV>,
    callback: NodeCallback<any>
  ): Promise<void> {
    const res = await this.conn.GetManyValsByKeys(this, keys, options);
    return this.nextTick(callback, null, res);
  }

  async _put(
    key: SQLK,
    val: SQLV,
    options: AbstractPutOptions<SQLK, SQLV>,
    callback: NodeCallback<any>
  ): Promise<void> {
    await this.conn.Put(this, key, val, options);
    return this.nextTick(callback);
  }

  async _del(
    key: SQLK,
    options: AbstractDelOptions<SQLK>,
    callback: NodeCallback<any>
  ): Promise<void> {
    await this.conn.Delete(this, key, options);
    return this.nextTick(callback);
  }

  async _clear(
    options: AbstractClearOptions<SQLK>,
    callback: NodeCallback<any>
  ): Promise<void> {
    await this.conn.Clear(this, options);
    return this.nextTick(callback);
  }

  async _batch(
    operations: AbstractBatchOperation<
      SQLiteLevel<SQLK, SQLV>,
      SQLK,
      SQLV
    >[],
    options: AbstractBatchOptions<SQLK, SQLV>,
    callback: NodeCallback<any>
  ): Promise<void> {
    const batch = await this.conn.Batch(this);
    await batch(operations, options);
    return this.nextTick(callback, null, { db: this });
  }

  // SUBLEVEL

  _sublevel<K = SQLK, V = SQLV>(
    name: string,
    options: AbstractSublevelOptions<K, V>
  ) {
    return new SQLiteSublevel(this, name, options);
  }

  // ITERATORS

  _iterator(options: AbstractIteratorOptions<SQLK, SQLV>) {
    return new SQLiteIterator<SQLK, SQLV>(this.idx++, this, options);
  }

  _keys(options: AbstractKeyIteratorOptions<SQLK>) {
    return new SQLiteKeyIterator(this.idx++, this, options);
  }

  _values(options: AbstractValueIteratorOptions<SQLK, SQLV>) {
    return new SQLiteValueIterator(this.idx++, this, options);
  }

  //CHAINED BATCH

  _chainedBatch() {
    return new SQLiteChainedBatch(this);
  }
}
