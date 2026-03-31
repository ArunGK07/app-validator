import { mkdtemp, mkdir, rm, writeFile } from 'node:fs/promises';
import os from 'node:os';
import { join } from 'node:path';
import assert from 'node:assert/strict';
import test from 'node:test';

import { runNamingStandardValidator } from './naming-standard-validator.mjs';

function createExecuteOnlyConnectionStub() {
  return {
    cursor() {
      throw new Error('cursor() should not be used when execute() is available');
    },
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

test('runNamingStandardValidator compiles anonymous DECLARE blocks via a synthetic procedure body', async () => {
  const root = await mkdtemp(join(os.tmpdir(), 'app-validator-naming-validator-anon-'));
  const taskDir = join(root, '24696');
  const metadata = {
    id: 24696,
    num_turns: 1,
    dataset: 'world_bank_wdi',
    database: 'bigquery-public-data',
  };
  const executedSql = [];

  try {
    await mkdir(taskDir, { recursive: true });
    await writeFile(
      join(taskDir, '24696_turn1_4referenceAnswer.sql'),
      [
        'DECLARE',
        '  lv_year NUMBER := 2004;',
        'BEGIN',
        '  NULL;',
        'EXCEPTION',
        '  WHEN OTHERS THEN',
        '    NULL;',
        'END;',
        '/',
      ].join('\n'),
      'utf8',
    );

    const results = await runNamingStandardValidator('24696', taskDir, metadata, {
      connect: async () => ({
        async execute(sql) {
          executedSql.push(sql);
          if (/FROM user_errors/i.test(sql) || /FROM user_identifiers/i.test(sql)) {
            return { rows: [] };
          }
          return { rows: [] };
        },
        async close() {},
      }),
    });

    assert.equal(results.length, 1);
    assert.equal(results[0].status, 'PASS');

    const compiledProcedure = executedSql.find((sql) => /CREATE OR REPLACE PROCEDURE TMP_VALIDATE_/i.test(sql));
    assert.ok(compiledProcedure, 'expected a synthetic procedure compilation statement');
    assert.match(compiledProcedure, /CREATE OR REPLACE PROCEDURE TMP_VALIDATE_[\s\S]*\sIS\s+lv_year NUMBER := 2004;[\s\S]*BEGIN[\s\S]*END;$/i);
    assert.doesNotMatch(compiledProcedure, /\bBEGIN\s+DECLARE\b/i);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test('runNamingStandardValidator allows prompt-required top-level object names without prefix rewriting', async () => {
  const root = await mkdtemp(join(os.tmpdir(), 'app-validator-naming-validator-prompt-name-'));
  const taskDir = join(root, '25002');
  const metadata = {
    id: 25002,
    num_turns: 1,
    dataset: 'world_bank_wdi',
    database: 'bigquery-public-data',
  };

  try {
    await mkdir(taskDir, { recursive: true });
    await writeFile(
      join(taskDir, '25002_turn1_1user.txt'),
      [
        'Validate one prompt-required routine name.',
        'Requirements:',
        'Procedure Name:',
        'calculate_regional_hub_metrics',
        '',
        'Parameters:',
        '\tp_city_name - IN - VARCHAR2 -- city name to analyze',
        '',
        'Output:',
        '\tStarting Message: Starting analysis for city: [city_name]',
        '',
        'Exception Handling:',
        '\tOther Exception : Unexpected error occurred',
      ].join('\n'),
      'utf8',
    );
    await writeFile(
      join(taskDir, '25002_turn1_4referenceAnswer.sql'),
      [
        'CREATE OR REPLACE PROCEDURE calculate_regional_hub_metrics(',
        '  p_city_name IN VARCHAR2',
        ') IS',
        'BEGIN',
        '  NULL;',
        'EXCEPTION',
        '  WHEN OTHERS THEN',
        '    NULL;',
        'END;',
        '/',
      ].join('\n'),
      'utf8',
    );

    const results = await runNamingStandardValidator('25002', taskDir, metadata, {
      connect: async () => createExecuteOnlyConnectionStub(),
    });

    assert.equal(results.length, 1);
    assert.equal(results[0].status, 'PASS');
    assert.equal(results[0].item, 'Naming Standard');
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test('runNamingStandardValidator normalizes Spider schema names for CURRENT_SCHEMA session setup', async () => {
  const root = await mkdtemp(join(os.tmpdir(), 'app-validator-naming-validator-schema-'));
  const taskDir = join(root, '15803');
  const metadata = {
    id: 15803,
    num_turns: 1,
    dataset: 'Spider 2.0-Lite',
    database: 'Db-IMDB',
  };
  const executedSql = [];

  try {
    await mkdir(taskDir, { recursive: true });
    await writeFile(
      join(taskDir, '15803_turn1_4referenceAnswer.sql'),
      ['CREATE OR REPLACE PROCEDURE sp_movie_check IS', 'BEGIN', '  NULL;', 'END;', '/'].join('\n'),
      'utf8',
    );

    const results = await runNamingStandardValidator('15803', taskDir, metadata, {
      connect: async () => ({
        async execute(sql) {
          executedSql.push(sql);
          if (/FROM user_errors/i.test(sql) || /FROM user_identifiers/i.test(sql)) {
            return { rows: [] };
          }
          return { rows: [] };
        },
        async close() {},
      }),
    });

    assert.equal(results.length, 1);
    assert.equal(results[0].status, 'PASS');
    assert.equal(executedSql[0], 'ALTER SESSION SET CURRENT_SCHEMA = DB_IMDB');
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});


