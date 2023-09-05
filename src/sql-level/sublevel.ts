import {
  AbstractBatchOperation,
  AbstractBatchOptions,
  AbstractClearOptions,
  AbstractDelOptions,
  AbstractGetManyOptions,
  AbstractGetOptions,
  AbstractIteratorOptions,
  AbstractKeyIteratorOptions,
  AbstractLevel,
  AbstractOpenOptions,
  AbstractPutOptions,
  AbstractSublevel,
  AbstractSublevelOptions,
  AbstractValueIteratorOptions,
  NodeCallback,
} from "abstract-level";
import { SQLD, SQLDB, SQLiteLevelSQL, SQLiteLevelSchema } from "./sql";
import { ModuleError } from "../core/error";
import {
  SQLiteIterator,
  SQLiteKeyIterator,
  SQLiteValueIterator,
  SomeSQLiteIterator,
} from "./iterator";
import { SQLiteChainedBatch } from "./chained-batch";

export class SQLiteSublevel<SQLK, SQLV> extends AbstractSublevel<
  SQLDB<any, any>,
  SQLD,
  SQLK,
  SQLV
> {
  constructor(
    db: SQLDB<any, any>,
    name: string,
    options: AbstractSublevelOptions<SQLK, SQLV>
  ) {
    super(db, name, options);
  }

  get idx(): number {
    return this.db.idx;
  }
  set idx(val: number) {
    this.db.idx = val;
  }
  get itcn(): Map<number, SomeSQLiteIterator<SQLK, SQLV>> {
    return this.db.itcn;
  }
  get conn(): SQLiteLevelSQL<SQLK, SQLV> {
    return this.db.conn;
  }

  // INSTANCE MANAGEMENT

  _open(
    options: AbstractOpenOptions,
    callback: NodeCallback<any>
  ): Promise<void> {
    return this.db._open(options, callback);
  }

  _close(callback: NodeCallback<any>): Promise<void> {
    return this.db._close(callback);
  }

  // DEFERED OPERATIONS
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
    operations: AbstractBatchOperation<SQLDB<SQLK, SQLV>, SQLK, SQLV>[],
    options: AbstractBatchOptions<SQLK, SQLV>,
    callback: NodeCallback<any>
  ): Promise<void> {
    const batch = await this.conn.Batch(this);
    await batch(operations, options);
    return this.nextTick(callback, null, { db: this });
  }

  _sublevel<K = SQLK, V = SQLV>(
    name: string,
    options: AbstractSublevelOptions<SQLK, SQLV>
  ): SQLiteSublevel<SQLK, SQLV> {
    return new SQLiteSublevel(this, name, options);
  }

  // ITERATORS
  _iterator(
    options: AbstractIteratorOptions<SQLK, SQLV>
  ): SQLiteIterator<SQLK, SQLV> {
    return new SQLiteIterator<SQLK, SQLV>(this.idx++, this, options);
  }

  _keys(options: AbstractKeyIteratorOptions<SQLK>) {
    return new SQLiteKeyIterator(this.idx++, this, options);
  }

  _values(options: AbstractValueIteratorOptions<SQLK, SQLV>) {
    return new SQLiteValueIterator(this.idx++, this, options);
  }

  // CHAINED BATCH
  _chainedBatch() {
    return new SQLiteChainedBatch(this);
  }
}
