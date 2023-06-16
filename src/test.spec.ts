import test from "tape";
import Suite from "abstract-level/test/index";
import { SQLiteLevel } from "./sql-level/level";
import { join as pathJoin } from "path";

Promise.all([import("tempy")]).then((imports) => {
  const { temporaryDirectory } = imports[0];

  Suite({
    test,
    factory(options: any) {
      const dbPath = pathJoin(temporaryDirectory(), "test.db");
      return new SQLiteLevel(dbPath, options);
    },
  });
});