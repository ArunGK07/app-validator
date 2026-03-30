import fs from 'fs';
import path from 'path';
import { PLSQL_CONSTRUCT_CATALOG } from './plsql-construct-catalog.mjs';

const target = 'D:/Turing/Projects/workspace/task-output/15761/15761_turn2_4referenceAnswer.sql';

try {
  const text = await fs.promises.readFile(target, 'utf8');

  console.log('Testing patterns against:', target);

  for (const item of PLSQL_CONSTRUCT_CATALOG) {
    const { id, label, pattern } = item;
    try {
      const re = pattern instanceof RegExp ? pattern : new RegExp(pattern, 'i');
      const m = text.match(re);
      if (m) {
        const snippet = String(m[0]).replace(/\r?\n/g, '\\n').slice(0, 300);
        console.log(`${id} | ${label} | Matched snippet: ${snippet}`);
      }
    } catch (err) {
      console.error('Pattern error', id, label, err.message);
    }
  }
} catch (err) {
  console.error('Failed to read target file:', target, err.message);
  process.exitCode = 2;
}
