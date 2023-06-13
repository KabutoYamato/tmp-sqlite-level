import {
  AbstractIterator,
  AbstractIteratorOptions,
  AbstractKeyIterator,
  AbstractKeyIteratorOptions,
  AbstractValueIterator,
  AbstractValueIteratorOptions,
  NodeCallback,
} from "abstract-level";
import { DataRecord, SQLD, SQLDB } from "./sql";

export class SQLiteIterator<SQLK, SQLV> extends AbstractIterator<
  SQLDB<any, any>,
  SQLK,
  SQLV
> {
  private iterator: IterableIterator<DataRecord<SQLD, SQLK, SQLV>>;

  constructor(
    readonly idx: number,
    db: SQLDB<any, any>,
    options: AbstractIteratorOptions<SQLK, SQLV>
  ) {
    super(db, options);
    this.iterator = this.db.conn.Iterator(db, options);
    this.db.itcn.set(this.idx, this);
  }

  _nextv(size: number, _options: object, callback: NodeCallback<any>) {
    const entries: [SQLK?, SQLV?][] = [];
    let limit = this.limit - this.count;
    let i = 0;
    while (true) {
      const val = this.iterator.next();
      if (val.done) break;

      entries.push([val.value.key, val.value.val]);
      i++;
      if (i >= size || i >= limit) break;
    }

    return this.db.nextTick(callback, null, entries);
  }

  _next(callback: NodeCallback<any>) {
    const val = this.iterator.next();
    if (val.done) {
      return this.db.nextTick(callback, null);
    }

    return this.db.nextTick(callback, null, val.value.key, val.value.val);
  }

  _close(callback: NodeCallback<any>) {
    if (this.iterator.return) {
      this.iterator.return();
    }
    this.db.itcn.delete(this.idx);
    return this.db.nextTick(callback);
  }

  _all(_options: object, callback: NodeCallback<any>) {
    const entries: [SQLK?, SQLV?][] = [];
    let limit = this.limit - this.count;
    let i = 0;
    for (const val of this.iterator) {
      entries.push([val.key, val.val]);
      i++;
      if (i === limit) {
        break;
      }
    }

    if (this.iterator.return) {
      this.iterator.return();
    }

    return this.db.nextTick(callback, null, entries);
  }
}

export class SQLiteKeyIterator<SQLK> extends AbstractKeyIterator<
  SQLDB<any, any>,
  SQLK
> {
  private iterator: IterableIterator<DataRecord<SQLD, SQLK, any>>;

  constructor(
    readonly idx: number,
    db: SQLDB<any, any>,
    options: AbstractKeyIteratorOptions<SQLK>
  ) {
    super(db, options);
    this.iterator = this.db.conn.Iterator(db, options);
    this.db.itcn.set(this.idx, this);
  }

  _next(callback: NodeCallback<SQLK>) {
    const val = this.iterator.next();
    if (val.done) {
      return this.db.nextTick(callback, null);
    }

    return this.db.nextTick(callback, null, val.value.key);
  }

  _nextv(size: number, _options: object, callback: NodeCallback<SQLK[]>) {
    const entries: SQLK[] = [];
    let limit = this.limit - this.count;
    let i = 0;
    while (true) {
      const val = this.iterator.next();
      if (val.done) break;

      entries.push(val.value.key);
      i++;
      if (i >= size || i >= limit) break;
    }

    return this.db.nextTick(callback, null, entries);
  }

  _close(callback: NodeCallback<any>) {
    if (this.iterator.return) {
      this.iterator.return();
    }
    this.db.itcn.delete(this.idx);
    return this.db.nextTick(callback);
  }
  _all(_options: object, callback: NodeCallback<SQLK[]>) {
    const entries: SQLK[] = [];
    for (const val of this.iterator) {
      entries.push(val.key);
    }

    if (this.iterator.return) {
      this.iterator.return();
    }

    return this.db.nextTick(callback, null, entries);
  }
}

export class SQLiteValueIterator<SQLK, SQLV> extends AbstractValueIterator<
  SQLDB<any, any>,
  SQLK,
  SQLV
> {
  private iterator: IterableIterator<DataRecord<SQLD, SQLK, SQLV>>;

  constructor(
    readonly idx: number,
    db: SQLDB<any, any>,
    options: AbstractValueIteratorOptions<SQLK, SQLV>
  ) {
    super(db, options);
    this.iterator = this.db.conn.Iterator(db, options);
    this.db.itcn.set(this.idx, this);
  }

  _nextv(size: number, _options: object, callback: NodeCallback<SQLV[]>) {
    const entries: SQLV[] = [];
    let limit = this.limit - this.count;
    let i = 0;
    while (true) {
      const val = this.iterator.next();
      if (val.done) break;

      entries.push(val.value.val);
      i++;
      if (i >= size || i >= limit) break;
    }

    return this.db.nextTick(callback, null, entries);
  }

  _next(callback: NodeCallback<any>) {
    const val = this.iterator.next();
    if (val.done) {
      return this.db.nextTick(callback, null);
    }

    return this.db.nextTick(callback, null, val.value.val);
  }

  _close(callback: NodeCallback<any>) {
    if (this.iterator.return) {
      this.iterator.return();
    }
    this.db.itcn.delete(this.idx);
    return this.db.nextTick(callback);
  }

  _all(_options: object, callback: NodeCallback<any>) {
    const entries: SQLV[] = [];
    for (const val of this.iterator) {
      entries.push(val.val);
    }

    if (this.iterator.return) {
      this.iterator.return();
    }

    return this.db.nextTick(callback, null, entries);
  }
}

export type SomeSQLiteIterator<SQLK, SQLV> =
  | SQLiteIterator<SQLK, SQLV>
  | SQLiteKeyIterator<SQLK>
  | SQLiteValueIterator<SQLK, SQLV>;
