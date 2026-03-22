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

test('refreshTaskTestCases preserves terminating semicolons for PL/SQL blocks', async () => {
  const root = await mkdtemp(join(os.tmpdir(), 'app-validator-testcase-runner-plsql-'));
  const taskDir = join(root, '24696');
  const metadata = {
    id: 24696,
    num_turns: 1,
    dataset: 'world_bank_wdi',
    database: 'bigquery-public-data',
  };

  const dbmsOutputQueue = [];
  const executedSql = [];

  try {
    await mkdir(taskDir, { recursive: true });
    await writeFile(
      join(taskDir, '24696_turn1_4referenceAnswer.sql'),
      [
        'DECLARE',
        '  lv_value NUMBER := 1;',
        'BEGIN',
        "  DBMS_OUTPUT.PUT_LINE('reference compiled');",
        'END;',
        '/',
      ].join('\n'),
      'utf8',
    );
    await writeFile(
      join(taskDir, '24696_turn1_5testCases.sql'),
      [
        'Test Case 1:',
        'execution_instructions:',
        'BEGIN',
        "  DBMS_OUTPUT.PUT_LINE('instruction executed');",
        'END;',
        '/',
        'execution_result:',
        'old',
        '',
      ].join('\n'),
      'utf8',
    );

    const result = await refreshTaskTestCases('24696', taskDir, metadata, {}, {
      connectionFactory: async () => ({
        async execute(sql) {
          const trimmed = sql.trim();
          executedSql.push(trimmed);

          if (/DBMS_OUTPUT\.ENABLE/i.test(trimmed)) {
            return { rows: [] };
          }
          if (/DBMS_OUTPUT\.GET_LINE/i.test(trimmed)) {
            const line = dbmsOutputQueue.shift();
            if (line === undefined) {
              return { outBinds: { line: null, status: 1 } };
            }
            return { outBinds: { line, status: 0 } };
          }
          if (/^(DECLARE|BEGIN)\b/i.test(trimmed)) {
            assert.match(trimmed, /END;$/i);
            if (/instruction executed/i.test(trimmed)) {
              dbmsOutputQueue.push('instruction executed');
            }
            return { rows: [] };
          }
          return { rows: [] };
        },
        async commit() {},
        async close() {},
      }),
    });

    assert.equal(result.updatedFiles.length, 1);
    assert.ok(executedSql.some((sql) => /^DECLARE\b[\s\S]*END;$/i.test(sql)));
    assert.ok(executedSql.some((sql) => /^BEGIN\b[\s\S]*END;$/i.test(sql)));
    assert.match(await readFile(join(taskDir, '24696_turn1_5testCases.sql'), 'utf8'), /execution_result:\s*\ninstruction executed\n/);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test('refreshTaskTestCases ignores SQL*Plus directives in execution instructions', async () => {
  const root = await mkdtemp(join(os.tmpdir(), 'app-validator-testcase-runner-directives-'));
  const taskDir = join(root, '30001');
  const metadata = {
    id: 30001,
    num_turns: 1,
    dataset: 'world_bank_wdi',
    database: 'bigquery-public-data',
  };

  const executedSql = [];

  try {
    await mkdir(taskDir, { recursive: true });
    await writeFile(
      join(taskDir, '30001_turn1_4referenceAnswer.sql'),
      [
        'BEGIN',
        '  NULL;',
        'END;',
        '/',
      ].join('\n'),
      'utf8',
    );
    await writeFile(
      join(taskDir, '30001_turn1_5testCases.sql'),
      [
        'Test Case 1:',
        'execution_instructions:',
        'SET SERVEROUTPUT ON;',
        'PROMPT about to run block',
        'BEGIN',
        '  NULL;',
        'END;',
        '/',
        'execution_result:',
        'old',
        '',
      ].join('\n'),
      'utf8',
    );

    const result = await refreshTaskTestCases('30001', taskDir, metadata, {}, {
      connectionFactory: async () => ({
        async execute(sql) {
          const trimmed = sql.trim();
          executedSql.push(trimmed);
          if (/DBMS_OUTPUT\.ENABLE/i.test(trimmed)) {
            return { rows: [] };
          }
          if (/DBMS_OUTPUT\.GET_LINE/i.test(trimmed)) {
            return { outBinds: { line: null, status: 1 } };
          }
          assert.doesNotMatch(trimmed, /^(SET|PROMPT)\b/i);
          return { rows: [] };
        },
        async commit() {},
        async close() {},
      }),
    });

    assert.equal(result.updatedFiles.length, 1);
    assert.ok(executedSql.some((sql) => /^BEGIN\b[\s\S]*END;$/i.test(sql)));
    assert.ok(executedSql.every((sql) => !/^(SET|PROMPT)\b/i.test(sql)));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test('refreshTaskTestCases handles SQL*Plus block separators after leading comments', async () => {
  const root = await mkdtemp(join(os.tmpdir(), 'app-validator-testcase-runner-comment-block-'));
  const taskDir = join(root, '30004');
  const metadata = {
    id: 30004,
    num_turns: 1,
    dataset: 'world_bank_wdi',
    database: 'bigquery-public-data',
  };

  const executedSql = [];

  try {
    await mkdir(taskDir, { recursive: true });
    await writeFile(
      join(taskDir, '30004_turn1_4referenceAnswer.sql'),
      [
        '-- leading comment before object type',
        'CREATE OR REPLACE TYPE rec_comment_type AS OBJECT (',
        '  id NUMBER',
        ');',
        '/',
        '',
        'CREATE OR REPLACE TYPE BODY rec_comment_type AS',
        'END rec_comment_type;',
        '/',
      ].join('\n'),
      'utf8',
    );
    await writeFile(
      join(taskDir, '30004_turn1_5testCases.sql'),
      [
        'Test Case 1:',
        'execution_instructions:',
        'SELECT 1 FROM dual;',
        'execution_result:',
        'old',
        '',
      ].join('\n'),
      'utf8',
    );

    const result = await refreshTaskTestCases('30004', taskDir, metadata, {}, {
      connectionFactory: async () => ({
        async execute(sql) {
          const trimmed = sql.trim();
          executedSql.push(trimmed);
          if (/DBMS_OUTPUT\.ENABLE/i.test(trimmed)) {
            return { rows: [] };
          }
          if (/DBMS_OUTPUT\.GET_LINE/i.test(trimmed)) {
            return { outBinds: { line: null, status: 1 } };
          }
          if (/^SELECT\s+1\s+FROM\s+dual$/i.test(trimmed)) {
            return { rows: [[1]] };
          }
          return { rows: [] };
        },
        async commit() {},
        async close() {},
      }),
    });

    assert.equal(result.updatedFiles.length, 1);
    assert.ok(executedSql.some((sql) => /^-- leading comment before object type[\s\S]*CREATE OR REPLACE TYPE rec_comment_type AS OBJECT/i.test(sql)));
    assert.ok(executedSql.some((sql) => /^CREATE OR REPLACE TYPE BODY rec_comment_type AS[\s\S]*END rec_comment_type;$/i.test(sql)));
    assert.ok(executedSql.every((sql) => !/^\//.test(sql)));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test('refreshTaskTestCases keeps a blank-line-prefixed PL/SQL block intact', async () => {
  const root = await mkdtemp(join(os.tmpdir(), 'app-validator-testcase-runner-leading-blank-'));
  const taskDir = join(root, '30002');
  const metadata = {
    id: 30002,
    num_turns: 1,
    dataset: 'world_bank_wdi',
    database: 'bigquery-public-data',
  };

  const executedSql = [];

  try {
    await mkdir(taskDir, { recursive: true });
    await writeFile(
      join(taskDir, '30002_turn1_4referenceAnswer.sql'),
      [
        'BEGIN',
        '  NULL;',
        'END;',
        '/',
      ].join('\n'),
      'utf8',
    );
    await writeFile(
      join(taskDir, '30002_turn1_5testCases.sql'),
      [
        'Test Case 1:',
        'execution_instructions:',
        '',
        'SET SERVEROUTPUT ON;',
        'BEGIN',
        '  sample_proc(1);',
        'END;',
        '/',
        'execution_result:',
        'old',
        '',
      ].join('\n'),
      'utf8',
    );

    const result = await refreshTaskTestCases('30002', taskDir, metadata, {}, {
      connectionFactory: async () => ({
        async execute(sql) {
          const trimmed = sql.trim();
          executedSql.push(trimmed);
          if (/DBMS_OUTPUT\.ENABLE/i.test(trimmed)) {
            return { rows: [] };
          }
          if (/DBMS_OUTPUT\.GET_LINE/i.test(trimmed)) {
            return { outBinds: { line: null, status: 1 } };
          }
          return { rows: [] };
        },
        async commit() {},
        async close() {},
      }),
    });

    assert.equal(result.updatedFiles.length, 1);
    assert.ok(executedSql.some((sql) => /^BEGIN\s+sample_proc\(1\);\s+END;$/i.test(sql)));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test('refreshTaskTestCases converts EXEC directives into runnable PL/SQL blocks', async () => {
  const root = await mkdtemp(join(os.tmpdir(), 'app-validator-testcase-runner-exec-'));
  const taskDir = join(root, '30003');
  const metadata = {
    id: 30003,
    num_turns: 1,
    dataset: 'world_bank_wdi',
    database: 'bigquery-public-data',
  };

  const executedSql = [];

  try {
    await mkdir(taskDir, { recursive: true });
    await writeFile(
      join(taskDir, '30003_turn1_4referenceAnswer.sql'),
      [
        'CREATE OR REPLACE PROCEDURE sp_validate_wdi_data (p_year IN NUMBER) IS',
        'BEGIN',
        '  NULL;',
        'END;',
        '/',
      ].join('\n'),
      'utf8',
    );
    await writeFile(
      join(taskDir, '30003_turn1_5testCases.sql'),
      [
        'Test Case 1:',
        'execution_instructions:',
        'SET SERVEROUTPUT ON;',
        'EXEC sp_validate_wdi_data(9999);',
        'execution_result:',
        'old',
        '',
      ].join('\n'),
      'utf8',
    );

    const result = await refreshTaskTestCases('30003', taskDir, metadata, {}, {
      connectionFactory: async () => ({
        async execute(sql) {
          const trimmed = sql.trim();
          executedSql.push(trimmed);
          if (/DBMS_OUTPUT\.ENABLE/i.test(trimmed)) {
            return { rows: [] };
          }
          if (/DBMS_OUTPUT\.GET_LINE/i.test(trimmed)) {
            return { outBinds: { line: null, status: 1 } };
          }
          return { rows: [] };
        },
        async commit() {},
        async close() {},
      }),
    });

    assert.equal(result.updatedFiles.length, 1);
    assert.ok(executedSql.some((sql) => /^BEGIN sp_validate_wdi_data\(9999\); END;$/i.test(sql)));
    assert.ok(executedSql.every((sql) => !/^EXEC(?:UTE)?\b/i.test(sql)));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});


