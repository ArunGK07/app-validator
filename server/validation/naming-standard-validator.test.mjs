import { mkdtemp, mkdir, rm, writeFile } from 'node:fs/promises';
import os from 'node:os';
import { join } from 'node:path';
import assert from 'node:assert/strict';
import test from 'node:test';

import { runNamingStandardValidator } from './naming-standard-validator.mjs';

function createExecuteOnlyConnectionStub() {
  return {
    async execute(sql) {
      if (/FROM user_errors/i.test(sql) || /FROM user_identifiers/i.test(sql)) {
        return { rows: [] };
      }
      return { rows: [] };
    },
    async close() {},
  };
}

test('runNamingStandardValidator supports Node oracledb-style connections with execute()', async () => {
  const root = await mkdtemp(join(os.tmpdir(), 'app-validator-naming-validator-'));
  const taskDir = join(root, '25001');
  const metadata = {
    id: 25001,
    num_turns: 1,
    dataset: 'world_bank_wdi',
    database: 'bigquery-public-data',
  };

  try {
    await mkdir(taskDir, { recursive: true });
    await writeFile(
      join(taskDir, '25001_turn1_4referenceAnswer.sql'),
      ['CREATE OR REPLACE PROCEDURE sp_world_bank_check IS', 'BEGIN', '  NULL;', 'END;', '/'].join('\n'),
      'utf8',
    );

    const results = await runNamingStandardValidator('25001', taskDir, metadata, {
      connect: async () => createExecuteOnlyConnectionStub(),
    });

    assert.equal(results.length, 1);
    assert.equal(results[0].validator, 'NamingStandardValidator');
    assert.equal(results[0].status, 'PASS');
    assert.equal(results[0].item, 'Naming Standard');
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
