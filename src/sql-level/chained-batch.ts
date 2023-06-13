import {
  AbstractBatchDelOperation,
  AbstractBatchOperation,
  AbstractBatchPutOperation,
  AbstractChainedBatch,
  AbstractChainedBatchWriteOptions,
  AbstractDelOptions,
  AbstractPutOptions,
  NodeCallback,
} from "abstract-level";
import { SQLDB } from "./sql";

export class SQLiteChainedBatch<
  SQLL extends SQLDB<any, any>,
  SQLK,
  SQLV
> extends AbstractChainedBatch<SQLL, SQLK, SQLV> {
  private writes: AbstractBatchOperation<SQLL, SQLK, SQLV>[];

  constructor(db: SQLL) {
    super(db);
    this.writes = [];
  }

  _clear() {
    this.writes = [];
  }

  _put(key: SQLK, value: SQLV, options: AbstractPutOptions<SQLK, SQLV>) {
    this.writes.push({
      key,
      value,
      type: "put",
      keyEncoding: options.keyEncoding,
      valueEncoding: options.valueEncoding,
    } as AbstractBatchPutOperation<SQLL, SQLK, SQLV>);
  }

  _del(key: SQLK, options: AbstractDelOptions<SQLK>) {
    this.writes.push({
      key,
      type: "del",
      keyEncoding: options.keyEncoding,
    } as AbstractBatchDelOperation<SQLL, SQLK>);
  }

  async _write(
    options: AbstractChainedBatchWriteOptions,
    callback: NodeCallback<any>
  ): Promise<void> {
    const batch = await this.db.conn.Batch(this.db);

    await batch(this.writes, options);

    return this.db.nextTick(callback);
  }

  async _close(callback: NodeCallback<any>) {
    this._clear();
    return this.db.nextTick(callback);
  }
}
