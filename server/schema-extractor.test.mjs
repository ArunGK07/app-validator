import { mkdtemp, mkdir, readFile, rm, writeFile } from 'node:fs/promises';
import os from 'node:os';
import { join } from 'node:path';
import assert from 'node:assert/strict';
import test from 'node:test';

import { generateTaskSchemaArtifact } from './schema-extractor.mjs';

test('generateTaskSchemaArtifact writes only the shared schema cache file', async () => {
  const root = await mkdtemp(join(os.tmpdir(), 'app-validator-schema-'));
  const taskDir = join(root, '24696');
  const sharedRoot = join(root, 'schema-cache');

  const connection = {
    async execute(sql, binds = {}) {
      if (sql.includes('FROM ALL_TABLES')) {
        return { rows: [['DEPT'], ['EMP']] };
      }

      if (sql.includes('FROM ALL_TAB_COLUMNS') && binds.table_name === 'DEPT') {
        return {
          rows: [
            ['DEPTNO', 'NUMBER', 'N', 22, 4, 0, 1],
            ['DNAME', 'VARCHAR2', 'Y', 14, null, null, 2],
          ],
        };
      }

      if (sql.includes('FROM ALL_TAB_COLUMNS') && binds.table_name === 'EMP') {
        return {
          rows: [
            ['EMPNO', 'NUMBER', 'N', 22, 4, 0, 1],
            ['DEPTNO', 'NUMBER', 'Y', 22, 4, 0, 2],
          ],
        };
      }

      if (sql.includes("CONSTRAINT_TYPE = 'P'") && binds.table_name === 'DEPT') {
        return { rows: [['PK_DEPT']] };
      }

      if (sql.includes("CONSTRAINT_TYPE = 'P'") && binds.table_name === 'EMP') {
        return { rows: [['PK_EMP']] };
      }

      if (sql.includes('FROM ALL_CONS_COLUMNS') && binds.constraint_name === 'PK_DEPT') {
        return { rows: [['DEPTNO']] };
      }

      if (sql.includes('FROM ALL_CONS_COLUMNS') && binds.constraint_name === 'PK_EMP') {
        return { rows: [['EMPNO']] };
      }

      if (sql.includes("CONSTRAINT_TYPE = 'R'") && binds.table_name === 'DEPT') {
        return { rows: [] };
      }

      if (sql.includes("CONSTRAINT_TYPE = 'R'") && binds.table_name === 'EMP') {
        return {
          rows: [['FK_EMP_DEPT', 'DEPTNO', 'GNOMAD', 'PK_DEPT', 'DEPT', 'DEPTNO']],
        };
      }

      if (sql.includes('SELECT COUNT(*) FROM GNOMAD.DEPT')) {
        return { rows: [[4]] };
      }

      if (sql.includes('SELECT COUNT(*) FROM GNOMAD.EMP')) {
        return { rows: [[14]] };
      }

      throw new Error(`Unexpected SQL: ${sql}`);
    },
    async close() {},
  };

  try {
    const result = await generateTaskSchemaArtifact(
      {
        taskId: '24696',
        taskDir,
        metadata: {
          dataset: 'Spider 2.0-Lite',
          database: 'GNOMAD',
        },
      },
      {
        schemaCacheDir: sharedRoot,
      },
      {
        connectionFactory: async () => connection,
      },
    );

    assert.equal(result.schemaFile, 'spider_2_lite/GNOMAD.json');
    assert.equal(result.sharedSchemaFile, 'spider_2_lite/GNOMAD.json');
    assert.equal(result.schemaName, 'GNOMAD');
    assert.equal(result.profile, 'spider_2_lite');
    assert.equal(result.source, 'database');

    await assert.rejects(readFile(join(taskDir, '24696_0schema.json'), 'utf8'));

    const sharedSchema = JSON.parse(await readFile(join(sharedRoot, 'spider_2_lite', 'GNOMAD.json'), 'utf8'));

    assert.equal(sharedSchema.database, 'GNOMAD');
    assert.equal(sharedSchema.table_count, 2);
    assert.deepEqual(sharedSchema.tables.DEPT.primary_key, {
      constraint_name: 'PK_DEPT',
      columns: ['DEPTNO'],
    });
    assert.deepEqual(sharedSchema.tables.EMP.foreign_keys, [
      {
        constraint_name: 'FK_EMP_DEPT',
        columns: ['DEPTNO'],
        referenced_owner: 'GNOMAD',
        referenced_table: 'DEPT',
        referenced_columns: ['DEPTNO'],
      },
    ]);
    assert.deepEqual(sharedSchema.tables.DEPT.columns[0], ['DEPTNO', 'NUMBER(4)', 0, 1, 22, 4, 0]);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test('generateTaskSchemaArtifact routes spider snow tasks to the snow profile using database as schema', async () => {
  const root = await mkdtemp(join(os.tmpdir(), 'app-validator-schema-'));
  const taskDir = join(root, '9494');
  const sharedRoot = join(root, 'schema-cache');
  let receivedSchemaName = '';
  let receivedSourceDataset = '';
  let receivedSourceDatabase = '';

  const connection = {
    async execute(sql) {
      if (sql.includes('FROM ALL_TABLES')) {
        return { rows: [] };
      }

      throw new Error(`Unexpected SQL: ${sql}`);
    },
    async close() {},
  };

  try {
    const result = await generateTaskSchemaArtifact(
      {
        taskId: '9494',
        taskDir,
        metadata: {
          dataset: 'SPIDER 2.0-SNOW',
          database: 'SNOWDB',
        },
      },
      {
        schemaCacheDir: sharedRoot,
      },
      {
        connectionFactory: async ({ schemaName, sourceDataset, sourceDatabase }) => {
          receivedSchemaName = schemaName;
          receivedSourceDataset = sourceDataset;
          receivedSourceDatabase = sourceDatabase;
          return connection;
        },
      },
    );

    assert.equal(receivedSchemaName, 'SNOWDB');
    assert.equal(receivedSourceDataset, 'SPIDER 2.0-SNOW');
    assert.equal(receivedSourceDatabase, 'SNOWDB');
    assert.equal(result.schemaName, 'SNOWDB');
    assert.equal(result.profile, 'spider_2_snow');
    assert.equal(result.source, 'database');
    assert.equal(result.sharedSchemaFile, 'spider_2_snow/SNOWDB.json');
    assert.equal(result.schemaFile, 'spider_2_snow/SNOWDB.json');

    const sharedSchema = JSON.parse(await readFile(join(sharedRoot, 'spider_2_snow', 'SNOWDB.json'), 'utf8'));

    assert.equal(sharedSchema.database, 'SNOWDB');
    assert.equal(sharedSchema.table_count, 0);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test('generateTaskSchemaArtifact reuses the shared schema cache before reconnecting', async () => {
  const root = await mkdtemp(join(os.tmpdir(), 'app-validator-schema-'));
  const taskDir = join(root, '9418');
  const sharedFile = join(root, 'schema-cache', 'spider_2_lite', 'GNOMAD.json');
  let connectionFactoryCalled = false;

  try {
    await mkdir(join(root, 'schema-cache', 'spider_2_lite'), { recursive: true });
    await writeFile(sharedFile, '{"database":"GNOMAD","table_count":1,"tables":{}}');

    const result = await generateTaskSchemaArtifact(
      {
        taskId: '9418',
        taskDir,
        metadata: {
          dataset: 'Spider 2.0-Lite',
          database: 'GNOMAD',
        },
      },
      {
        schemaCacheDir: join(root, 'schema-cache'),
      },
      {
        connectionFactory: async () => {
          connectionFactoryCalled = true;
          throw new Error('Connection should not be used when cache exists.');
        },
      },
    );

    assert.equal(result.source, 'cache');
    assert.equal(result.schemaFile, 'spider_2_lite/GNOMAD.json');
    assert.equal(connectionFactoryCalled, false);
    await assert.rejects(readFile(join(taskDir, '9418_0schema.json'), 'utf8'));
    assert.equal(await readFile(sharedFile, 'utf8'), '{"database":"GNOMAD","table_count":1,"tables":{}}');
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
