import { mkdtemp, mkdir, readFile, rm, writeFile } from 'node:fs/promises';
import os from 'node:os';
import { join } from 'node:path';
import assert from 'node:assert/strict';
import test from 'node:test';

import { refreshTaskTestCases } from './testcase-runner.mjs';

test('refreshTaskTestCases supports execute-only Oracle connections', async () => {
  const root = await mkdtemp(join(os.tmpdir(), 'app-validator-testcase-runner-'));
  const taskDir = join(root, '9462');
  const metadata = {
    id: 9462,
    num_turns: 1,
    dataset: 'sample',
    database: 'bigquery-public-data',
  };

  const dbmsOutputQueue = [];
  const executedSql = [];

  try {
    await mkdir(taskDir, { recursive: true });
    await writeFile(
      join(taskDir, '9462_turn1_4referenceAnswer.sql'),
      [
        'CREATE OR REPLACE PROCEDURE sp_do_work IS',
        'BEGIN',
        '  NULL;',
        'END;',
        '/',
      ].join('\n'),
      'utf8',
    );
    await writeFile(
      join(taskDir, '9462_turn1_5testCases.sql'),
      ['Test Case 1:', 'execution_instructions:', 'SELECT 1 FROM dual;', 'execution_result:', 'old', ''].join('\n'),
      'utf8',
    );

    const result = await refreshTaskTestCases('9462', taskDir, metadata, {}, {
      connectionFactory: async () => ({
        cursor() {
          throw new Error('cursor() should not be used when execute() is available');
        },
        async execute(sql) {
          executedSql.push(sql.trim());
          if (/DBMS_OUTPUT\.ENABLE/i.test(sql)) {
            return { rows: [] };
          }
          if (/DBMS_OUTPUT\.GET_LINE/i.test(sql)) {
            const line = dbmsOutputQueue.shift();
            if (line === undefined) {
              return { outBinds: { line: null, status: 1 } };
            }
            return { outBinds: { line, status: 0 } };
          }
          if (/^SELECT\s+1\s+FROM\s+dual$/i.test(sql.trim())) {
            return { rows: [[1]] };
          }
          return { rows: [] };
        },
        async commit() {},
        async close() {},
      }),
    });

    assert.equal(result.updatedFiles.length, 1);
    assert.match(await readFile(join(taskDir, '9462_turn1_5testCases.sql'), 'utf8'), /execution_result:\s*\n1\n/);
    assert.ok(executedSql.some((sql) => /^SELECT\s+1\s+FROM\s+dual$/i.test(sql)));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

