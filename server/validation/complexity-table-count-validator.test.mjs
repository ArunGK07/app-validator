import { mkdtemp, mkdir, rm, writeFile } from 'node:fs/promises';
import os from 'node:os';
import { join } from 'node:path';
import assert from 'node:assert/strict';
import test from 'node:test';

import { runComplexityTableCountValidator } from './complexity-table-count-validator.mjs';

test('runComplexityTableCountValidator allows advanced tasks to use more than five tables', async () => {
  const root = await mkdtemp(join(os.tmpdir(), 'app-validator-complexity-'));
  const taskDir = join(root, '32001');
  const metadata = { id: 32001, num_turns: 1, complexity: 'advanced' };

  try {
    await mkdir(taskDir, { recursive: true });
    await writeFile(
      join(taskDir, '32001_turn1_2tables.txt'),
      [
        'ORACLE_SQL.INVENTORY,',
        'ORACLE_SQL.LOCATIONS,',
        'ORACLE_SQL.PICKING_LOG,',
        'ORACLE_SQL.PRODUCT_MINIMUMS,',
        'ORACLE_SQL.PRODUCTS,',
        'ORACLE_SQL.PURCHASES',
      ].join('\n'),
      'utf8',
    );

    const results = await runComplexityTableCountValidator('32001', taskDir, metadata);

    assert.ok(results.some((entry) => entry.ruleId === 'count_aligned' && entry.status === 'PASS'));
    assert.ok(!results.some((entry) => entry.ruleId === 'count_mismatch' && entry.status === 'FAIL'));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
