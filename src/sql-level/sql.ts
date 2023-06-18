import SQLite from "better-sqlite3";
import { ModuleError } from "../core/error";
import type {
  AbstractBatchOperation,
  AbstractBatchOptions,
  AbstractClearOptions,
  AbstractDelOptions,
  AbstractGetManyOptions,
  AbstractGetOptions,
  AbstractIteratorOptions,
  AbstractKeyIteratorOptions,
  AbstractOpenOptions,
  AbstractPutOptions,
  AbstractValueIteratorOptions,
  Transcoder,
} from "abstract-level";
import type { Database,Transaction } from "better-sqlite3";
import type { RangeOptions } from "abstract-level/types/interfaces";
import { SQLiteLevel } from "./level";
import { SQLiteSublevel } from "./sublevel";

export type SQLD = string;

export type SQLDB<SQLK, SQLV> =
  | SQLiteLevel<SQLK, SQLV>
  | SQLiteSublevel<SQLK, SQLV>;

export type PrefixRangeOptions<K> = RangeOptions<K> &{sub?:string}

interface PreparedQuery {
  sql: string;
  prm: (string | number | boolean | object)[];
}

export type DataRecord<SQLE extends SQLD, SQLK = SQLE, SQLV = SQLE> = {
  key: SQLK;
  val: SQLV;
};

type EncOpt<TIn> = string | Transcoder.PartialEncoder<TIn> | undefined;
type DecOpt<TOut> = string | Transcoder.PartialDecoder<TOut> | undefined;

export class PrefixManager {
  private static _addPrefix(key: SQLD, sub?: SQLD): SQLD {
    if (sub === undefined) return key;
    return sub + key;
  }

  private static _rmPrefix(key: SQLD, sub?: SQLD): SQLD {
    if (sub === undefined) return key;
    return key.substring(sub.length);
  }

  static keyWithPrefix<SQLK>(ins: SQLDB<SQLK, any>, key: unknown): SQLD {
    return this._addPrefix(<string>key, ins.prefix);
  }

  static keyWithoutPrefix<SQLK>(
    ins: SQLDB<SQLK, any>,
    key: SQLD | undefined
  ): SQLD | undefined {
    return this._rmPrefix(key!,ins.prefix)
    
  }
}

export class RangeOptionsParser {
  static parse<SQLK>(
    ins: SQLDB<SQLK, any>,
    options: RangeOptions<SQLK>,
    encoding: EncOpt<SQLK>
  ): PrefixRangeOptions<SQLD> & {sub?:SQLD} {
    if (!encoding) return options as any;

    const range: PrefixRangeOptions<SQLD> = {
      limit: options.limit,
      reverse: options.reverse,
    };
    if (options.gt !== undefined) {
      range.gt = PrefixManager.keyWithPrefix(ins, options.gt);
    }
    if (options.gte !== undefined) {
      range.gte = PrefixManager.keyWithPrefix(ins, options.gte);
    }
    if (options.lt !== undefined) {
      range.lt = PrefixManager.keyWithPrefix(ins, options.lt);
    }
    if (options.lte !== undefined) {
      range.lte = PrefixManager.keyWithPrefix(ins, options.lte);
    }
    
    if (ins.prefix !== undefined) {
      range.sub = ins.prefix
    }
    

    return range;
  }
}

export interface SQLiteLevelSchema {
  /**
   * @description the name of the table where data is stored
   * @default "Data"
   */
  levelTable?: string;

  /**
   * @description the name of the idex that sqlite uses to find records
   * @default "unique_sub_key_idx"
   */
  idxName?: string;
}

export class SQLiteLevelSQL<SQLK, SQLV> {
  private schema: Required<SQLiteLevelSchema>;
  private db: Database | undefined;

  constructor(readonly loc: string, readonly sch: SQLiteLevelSchema) {
    this.schema = {
      levelTable: sch.levelTable ?? "Data",
      idxName: sch.idxName ?? "unique_sub_key_idx",
    };
  }

  private _ParseRange(options: PrefixRangeOptions<SQLD>): PreparedQuery {
    let sql = ``;
    const prm: any[] = [];

    // RANGE OPTIONS
    if (options.gte !== undefined || options.gt !== undefined) {
      const gtesql =
        options.gte !== undefined
          ? { op: ">=", vl: options.gte }
          : { op: ">", vl: options.gt };
      sql += `WHERE key ${gtesql.op} ?`;
      prm.push(gtesql.vl);
    }

    // RANGE OPTIONS
    if (options.lte !== undefined || options.lt !== undefined) {
      const st = sql === "" ? "WHERE" : " AND";
      const gtesql =
        options.lte !== undefined
          ? { op: "<=", vl: options.lte }
          : { op: "<", vl: options.lt };
      sql += `${st} key ${gtesql.op} ?`;
      prm.push(gtesql.vl);
    }

    
    if (options.sub) {
      let st = sql === "" ? "WHERE" : " AND";
      sql += `${st} key LIKE ? || '%'`;
      prm.push(options.sub);
    }
    

    // REVERSE OPTION
    sql += ` ORDER BY key ${options.reverse ? "DESC" : "ASC"} LIMIT ${
      options.limit ?? "-1"
    }`;

    return { sql, prm };
  }

  private *_GenerateIterator(
    ins: SQLDB<SQLK, SQLV>,
    options:
      | AbstractKeyIteratorOptions<SQLK>
      | AbstractValueIteratorOptions<SQLK, SQLV>
      | AbstractIteratorOptions<SQLK, SQLV>
  ): IterableIterator<DataRecord<SQLD, SQLK, SQLV>> {
    const db = new SQLite(this.loc, {
      readonly: true,
      fileMustExist: true,
    });

    const pOptions = RangeOptionsParser.parse(
      ins,
      options,
      options.keyEncoding
    );
    const where = this._ParseRange(pOptions);

    const baseIterator = db
      .prepare(`SELECT key,val FROM ${this.schema.levelTable} ${where.sql}`)
      .iterate(where.prm) as IterableIterator<DataRecord<SQLD>>;

    let val: IteratorResult<DataRecord<SQLD>, void>;
    try {
      while (true) {
        val = baseIterator.next();
        if (val.done) {
          break;
        }
        const res: DataRecord<SQLD, SQLK, SQLV> = {
          key: PrefixManager.keyWithoutPrefix(ins, val.value.key)! as SQLK,
          val: val.value.val! as SQLV
        };
        yield res;
      }
    } finally {
      const res = baseIterator.return?.();
      db.close();
      return res;
    }
  }

  private _DeferedDb(ins: SQLDB<SQLK, SQLV>): Promise<Database> {
    if (!this.db) {
      return new Promise((res, rej) => {
        ins.defer(() => {
          if (this.db) {
            res(this.db);
          } else {
            rej(
              new ModuleError("Database not open", { code: "LEVEL_NOT_OPEN" })
            );
          }
        });
      });
    }
    return Promise.resolve(this.db);
  }

  async OpenConnection(options: AbstractOpenOptions): Promise<void> {
    await Promise.resolve(0);
    await import("better-sqlite3/build/Release/better_sqlite3.node");

    if (!this.db) {
      this.db = new SQLite(this.loc);
      this.db.pragma("cache_size=10000");
      // this.db.pragma("locking_mode=EXCLUSIVE");
      // this.db.pragma("synchronous=NORMAL");
      this.db.pragma("page_size=4096");
      this.db.pragma("journal_mode=WAL");
      this.db.pragma("cache_size=5000");
    }

    this.db
      .prepare(
        `CREATE TABLE IF NOT EXISTS "${this.schema.levelTable}"(
            key TEXT,
            val TEXT
          )`
      )
      .run();

    this.db
      .prepare(
        `CREATE UNIQUE INDEX IF NOT EXISTS "${this.schema.idxName}" 
          ON "${this.schema.levelTable}"(key)`
      )
      .run();

    return;
  }

  async CloseConnection(): Promise<void> {
    await Promise.resolve(0);
    if (this.db) {
      this.db.close();
      this.db = undefined;
    }
    return;
  }

  async GetValByKey(
    ins: SQLDB<SQLK, SQLV>,
    key: SQLK,
    options: AbstractGetOptions<SQLK, SQLV>
  ): Promise<SQLD | undefined> {
    const db = await this._DeferedDb(ins);
    const res = db
      .prepare(
        `SELECT val
            FROM "${this.schema.levelTable}" 
            WHERE key=?`
      )
      .get(key) as { val: SQLD } | undefined;

    return res ? res.val : undefined;
  }

  async GetManyValsByKeys(
    ins: SQLDB<SQLK, SQLV>,
    keys: SQLD[],
    options: AbstractGetManyOptions<SQLK, SQLV>
  ): Promise<(SQLD | undefined)[]> {
    const db = await this._DeferedDb(ins);
    const valsMap: Map<SQLD, SQLD | undefined> = new Map<
      SQLD,
      SQLD | undefined
    >();
    const qRes = db
      .prepare(
        `SELECT key,val 
          FROM "${this.schema.levelTable}" 
          WHERE key IN(${keys.map((_key) => "?").join(",")})`
      )
      .all(keys) as DataRecord<SQLD>[];

    qRes.forEach((res) => {
      valsMap.set(res.key!, res.val!);
    });

    return keys.map((key) => valsMap.get(key));
  }

  async Put(
    ins: SQLDB<SQLK, SQLV>,
    key: SQLK,
    val: SQLV,
    options: AbstractPutOptions<SQLK, SQLV>
  ): Promise<void> {
    const db = await this._DeferedDb(ins);

    db.prepare(
      `INSERT INTO "${this.schema.levelTable}" (key,val) 
        VALUES (?,?) 
        ON CONFLICT(key) 
        DO UPDATE SET val=?`
    ).run(key, val, val);

    return;
  }

  async Delete(
    ins: SQLDB<SQLK, SQLV>,
    key: SQLK,
    options: AbstractDelOptions<SQLK>
  ) {
    const db = await this._DeferedDb(ins);
    db.prepare(
      `DELETE FROM "${this.schema.levelTable}" 
        WHERE key=?`
    ).run(key);
  }

  async Clear(ins: SQLDB<SQLK, SQLV>, options: AbstractClearOptions<SQLK>) {
    const db = await this._DeferedDb(ins);
    const pOptions = RangeOptionsParser.parse(
      ins,
      options,
      options.keyEncoding
    );
    const where = this._ParseRange(pOptions);
    db.prepare(`DELETE FROM "${this.schema.levelTable}" ${where.sql}`).run(
      where.prm
    );
  }

  async Batch(ins: SQLDB<SQLK, SQLV>):Promise<Transaction>{
    const db = await this._DeferedDb(ins);
    const transaction = db.transaction(
      async (
        operations: AbstractBatchOperation<SQLDB<SQLK, SQLV>, SQLK, SQLV>[],
        options?: AbstractBatchOptions<SQLK, SQLV>
      ) => {
        const tasks = operations.map(async (op) => {
          const db = <SQLDB<SQLK, SQLV>>op.sublevel ?? ins.db;

          const opt = {
            keyEncoding: options?.keyEncoding ?? op.keyEncoding,
            valueEncoding: options?.valueEncoding ??
              (<AbstractPutOptions<SQLK, SQLV>>op).valueEncoding,
          };

          if (op.type === "put") {
            await this.Put(db, op.key, op.value, opt);
          } else if (op.type === "del") {
            await this.Delete(db, op.key, opt);
          }
        });

        await Promise.all(tasks);

        return;
      }
    );

    return transaction;
  }

  Iterator(
    ins: SQLDB<SQLK, SQLV>,
    options:
      | AbstractKeyIteratorOptions<SQLK>
      | AbstractValueIteratorOptions<SQLK, SQLV>
      | AbstractIteratorOptions<SQLK, SQLV>
  ) {
    return this._GenerateIterator(ins, options);
  }
}
