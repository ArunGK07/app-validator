import { mkdtemp, mkdir, readFile, rm, writeFile } from 'node:fs/promises';
import os from 'node:os';
import { join } from 'node:path';
import assert from 'node:assert/strict';
import test from 'node:test';

import { runTaskWorkflowAction } from './task-workflows.mjs';
import { PLSQL_CONSTRUCT_CATALOG, PLSQL_REASONING_TYPE_CATALOG } from './generation/reference-data.mjs';

function createNamingConnectionStub() {
  return {
    cursor() {
      return {
        async execute(sql) {
          if (/FROM user_errors/i.test(sql) || /FROM user_identifiers/i.test(sql)) {
            return { rows: [] };
          }
          return { rows: [] };
        },
      };
    },
    async close() {},
  };
}

async function writeValidationFixture(taskDir, taskId = '9418') {
  await writeFile(
    join(taskDir, `${taskId}_1metadata.json`),
    JSON.stringify({
      id: Number(taskId),
      num_turns: 1,
      complexity: 'simple',
      required_procs_funcs_pkgs: false,
      required_anonymous_block: false,
      target_reasoning_types: [],
      dataset: 'sample',
      database: 'bigquery-public-data',
    }),
    'utf8',
  );
  await writeFile(
    join(taskDir, `${taskId}_turn1_1user.txt`),
    [
      'Requirements:',
      'Procedure Name:',
      '\tsp_do_work',
      'Parameters:',
      '\tp_input - IN - NUMBER -- input value',
      'Output:',
      '\tresult row is printed',
      'Exception Handling:',
      '\tOther Exception : Unexpected error occurred',
    ].join('\n'),
    'utf8',
  );
  await writeFile(join(taskDir, `${taskId}_turn1_2tables.txt`), 'SAMPLE.TABLE_A\nSAMPLE.TABLE_B\n', 'utf8');
  await writeFile(
    join(taskDir, `${taskId}_turn1_4referenceAnswer.sql`),
    [
      'CREATE OR REPLACE PROCEDURE sp_do_work(p_input IN NUMBER) IS',
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
}

async function writeWorkflowFixture(taskDir, taskId = '9462') {
  await writeFile(
    join(taskDir, `${taskId}_1metadata.json`),
    JSON.stringify({
      id: Number(taskId),
      num_turns: 1,
      complexity: 'simple',
      required_procs_funcs_pkgs: false,
      required_anonymous_block: false,
      target_reasoning_types: ['Data Retrieval'],
      dataset: 'sample',
      database: 'bigquery-public-data',
    }),
    'utf8',
  );
  await writeFile(join(taskDir, `${taskId}_turn1_1user.txt`), 'Requirements:\nProcedure Name:\nsp_do_work\n', 'utf8');
  await writeFile(join(taskDir, `${taskId}_turn1_2tables.txt`), 'SAMPLE.EMPLOYEES\n', 'utf8');
  await writeFile(join(taskDir, `${taskId}_turn1_3columns.txt`), 'SAMPLE.EMPLOYEES.employee_id\n', 'utf8');
  await writeFile(
    join(taskDir, `${taskId}_turn1_4referenceAnswer.sql`),
    'CREATE OR REPLACE PROCEDURE sp_do_work IS\n  l_employee_id NUMBER;\nBEGIN\n  SELECT employee_id INTO l_employee_id FROM employees;\nEND;\n/\n',
    'utf8',
  );
  await writeFile(
    join(taskDir, `${taskId}_turn1_5testCases.sql`),
    ['Test Case 1:', 'execution_instructions:', 'SELECT 1 FROM dual;', 'execution_result:', 'old', ''].join('\n'),
    'utf8',
  );
  await writeFile(join(taskDir, `${taskId}_turn1_6reasoningTypes.txt`), 'Data Retrieval\n', 'utf8');
  await writeFile(join(taskDir, `${taskId}_turn1_7plSqlConstructs.txt`), 'CREATE OR REPLACE PROCEDURE\n', 'utf8');
  await writeFile(
    join(taskDir, `${taskId}_existing_output.json`),
    JSON.stringify({
      task_id: Number(taskId),
      colabLink: 'https://labeling-o.turing.com/conversations/9462/view',
      response: {
        data: {
          prompt: {
            promptTurns: [
              {
                id: 'turn-1',
                promptIndex: 0,
                preferenceSignal: null,
                unratable: false,
                promptEvaluationFeedback: {
                  promptTurnEvaluation: [{ name: 'user', value: 'old prompt' }],
                },
              },
            ],
          },
        },
      },
    }),
    'utf8',
  );
}

test('runTaskWorkflowAction(validate) writes native validation reports and structured summaries', async () => {
  const root = await mkdtemp(join(os.tmpdir(), 'app-validator-task-workflows-'));
  const taskOutputDir = join(root, 'task-output');
  const taskDir = join(taskOutputDir, '9418');

  try {
    await mkdir(taskDir, { recursive: true });
    await writeValidationFixture(taskDir);

    const result = await runTaskWorkflowAction(
      'validate',
      '9418',
      {
        cookie: 'cookie=value',
        taskOutputDir,
        trainerProjectDir: 'D:\\Turing\\Projects\\workspace\\llm-trainer-project',
        trainerValidationReportsDir: join(root, 'validation-reports'),
      },
      {
        validationDependencies: {
          naming: {
            connect: async () => createNamingConnectionStub(),
          },
        },
      },
    );

    assert.equal(result.success, true);
    assert.equal(result.action, 'validate');
    assert.deepEqual(result.command, ['native-validation']);
    assert.equal(result.summary.validatorsFailed, 0);
    assert.equal(result.validators?.length, 3);
    assert.equal(result.reports?.master, '_validation/master_validator_task_9418.json');
    assert.equal(result.artifacts.length, 4);
    assert.match(result.logFile, /_logs[\\\/]validate-/);
    const masterReport = JSON.parse(await readFile(join(taskDir, '_validation', 'master_validator_task_9418.json'), 'utf8'));
    assert.equal(masterReport.summary.validatorsFailed, 0);
    assert.equal(masterReport.validators.length, 3);
    assert.ok(Array.isArray(masterReport.checklist));
    assert.ok(masterReport.checklist.length > 0);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test('runTaskWorkflowAction(validate) clears existing log files before launching the validator', async () => {
  const root = await mkdtemp(join(os.tmpdir(), 'app-validator-task-workflows-'));
  const taskOutputDir = join(root, 'task-output');
  const taskDir = join(taskOutputDir, '9418');
  const logsDir = join(taskDir, '_logs');
  const staleValidateLog = join(logsDir, 'validate-old.log');
  const stalePublishLog = join(logsDir, 'publish-old.log');
  const retainedPropertiesFile = join(logsDir, 'publish-cookie.properties');

  try {
    await mkdir(logsDir, { recursive: true });
    await writeValidationFixture(taskDir);
    await writeFile(staleValidateLog, 'old validate log', 'utf8');
    await writeFile(stalePublishLog, 'old publish log', 'utf8');
    await writeFile(retainedPropertiesFile, 'Cookie=keep\n', 'utf8');

    const result = await runTaskWorkflowAction(
      'validate',
      '9418',
      {
        cookie: 'cookie=value',
        taskOutputDir,
        trainerProjectDir: 'D:\\Turing\\Projects\\workspace\\llm-trainer-project',
        trainerValidationReportsDir: join(root, 'validation-reports'),
      },
      {
        validationDependencies: {
          naming: {
            connect: async () => createNamingConnectionStub(),
          },
        },
      },
    );

    await assert.rejects(readFile(staleValidateLog, 'utf8'));
    await assert.rejects(readFile(stalePublishLog, 'utf8'));
    assert.equal(await readFile(retainedPropertiesFile, 'utf8'), 'Cookie=keep\n');
    assert.match(await readFile(result.logFile, 'utf8'), /Validation run completed/);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test('runTaskWorkflowAction(generate-outputs) writes native analyzer artifacts', async () => {
  const root = await mkdtemp(join(os.tmpdir(), 'app-validator-task-workflows-'));
  const taskOutputDir = join(root, 'task-output');
  const schemaCacheDir = join(root, 'schema');
  const taskDir = join(taskOutputDir, '9462');

  try {
    await mkdir(taskDir, { recursive: true });
    await mkdir(join(schemaCacheDir, 'bigquery_public_data'), { recursive: true });
    await writeWorkflowFixture(taskDir);
    await writeFile(
      join(schemaCacheDir, 'bigquery_public_data', 'sample.json'),
      JSON.stringify({
        database: 'SAMPLE',
        tables: {
          EMPLOYEES: {
            columns: [['EMPLOYEE_ID'], ['FIRST_NAME']],
          },
        },
        _schema_definition: {
          column_format: ['name'],
        },
      }),
      'utf8',
    );

    const result = await runTaskWorkflowAction(
      'generate-outputs',
      '9462',
      {
        cookie: 'cookie=value',
        taskOutputDir,
        schemaCacheDir,
        trainerProjectDir: 'D:\\Turing\\Projects\\workspace\\llm-trainer-project',
      },
      {
        generateDependencies: {
          schemaGenerator: async () => ({ schemaFile: 'bigquery_public_data/sample.json' }),
          testcaseRunner: async () => {
            const testcasePath = join(taskDir, '9462_turn1_5testCases.sql');
            await writeFile(
              testcasePath,
              ['Test Case 1:', 'execution_instructions:', 'SELECT 1 FROM dual;', 'execution_result:', '1', ''].join('\n'),
              'utf8',
            );
            return { updatedFiles: [testcasePath], logLines: ['Turn 1: refreshed testcase output'] };
          },
        },
      },
    );

    assert.equal(result.success, true);
    assert.deepEqual(result.command, ['native-generate-outputs']);
    assert.match(await readFile(join(taskDir, '9462_turn1_6reasoningTypes.txt'), 'utf8'), /Data Retrieval/);
    const reasoningAudit = JSON.parse(await readFile(join(taskDir, '9462_turn1_6reasoningTypes.audit.json'), 'utf8'));
    assert.equal(reasoningAudit.totalItemsConsidered, PLSQL_REASONING_TYPE_CATALOG.length);
    assert.ok(Array.isArray(reasoningAudit.items));
    assert.equal(reasoningAudit.items.length, PLSQL_REASONING_TYPE_CATALOG.length);
    assert.ok(reasoningAudit.items.every((entry) => entry.considered === true));
    assert.ok(reasoningAudit.items.some((entry) => entry.label === 'Data Retrieval' && entry.matched));
    assert.match(await readFile(join(taskDir, '9462_turn1_7plSqlConstructs.txt'), 'utf8'), /CREATE OR REPLACE PROCEDURE/);
    const constructsAudit = JSON.parse(await readFile(join(taskDir, '9462_turn1_7plSqlConstructs.audit.json'), 'utf8'));
    assert.equal(constructsAudit.totalItemsConsidered, PLSQL_CONSTRUCT_CATALOG.length);
    assert.ok(Array.isArray(constructsAudit.items));
    assert.equal(constructsAudit.items.length, PLSQL_CONSTRUCT_CATALOG.length);
    assert.ok(constructsAudit.items.every((entry) => entry.considered === true));
    assert.ok(constructsAudit.items.some((entry) => entry.label === 'CREATE OR REPLACE PROCEDURE' && entry.matched));
    assert.match(await readFile(join(taskDir, '9462_turn1_5testCases.sql'), 'utf8'), /execution_result:\n1/);
    assert.match(await readFile(result.logFile, 'utf8'), /reasoning types considered/);
    assert.match(await readFile(result.logFile, 'utf8'), /PL\/SQL constructs considered/);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test('runTaskWorkflowAction(generate-outputs) returns a failed result and preserves the log when the native runner throws', async () => {
  const root = await mkdtemp(join(os.tmpdir(), 'app-validator-task-workflows-'));
  const taskOutputDir = join(root, 'task-output');
  const schemaCacheDir = join(root, 'schema');
  const taskDir = join(taskOutputDir, '9462');

  try {
    await mkdir(taskDir, { recursive: true });
    await mkdir(join(schemaCacheDir, 'bigquery_public_data'), { recursive: true });
    await writeWorkflowFixture(taskDir);
    await writeFile(
      join(schemaCacheDir, 'bigquery_public_data', 'sample.json'),
      JSON.stringify({
        database: 'SAMPLE',
        tables: {},
        _schema_definition: {
          column_format: ['name'],
        },
      }),
      'utf8',
    );

    const result = await runTaskWorkflowAction(
      'generate-outputs',
      '9462',
      {
        cookie: 'cookie=value',
        taskOutputDir,
        schemaCacheDir,
        trainerProjectDir: 'D:\\Turing\\Projects\\workspace\\llm-trainer-project',
      },
      {
        generateDependencies: {
          schemaGenerator: async () => ({ schemaFile: 'bigquery_public_data/sample.json' }),
          testcaseRunner: async () => {
            throw new Error('simulated testcase refresh crash');
          },
        },
      },
    );

    assert.equal(result.success, false);
    assert.equal(result.exitCode, 1);
    assert.deepEqual(result.command, ['native-generate-outputs']);
    assert.match(result.logFile, /_logs[\\\/]generate-outputs-/);
    assert.match(await readFile(result.logFile, 'utf8'), /Workflow action failed unexpectedly/);
    assert.match(await readFile(result.logFile, 'utf8'), /simulated testcase refresh crash/);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test('runTaskWorkflowAction(publish) uses the native GraphQL publisher', async () => {
  const root = await mkdtemp(join(os.tmpdir(), 'app-validator-task-workflows-'));
  const taskOutputDir = join(root, 'task-output');
  const taskDir = join(taskOutputDir, '9462');
  const requests = [];

  try {
    await mkdir(taskDir, { recursive: true });
    await writeWorkflowFixture(taskDir);

    const result = await runTaskWorkflowAction(
      'publish',
      '9462',
      {
        cookie: 'cookie=value',
        taskOutputDir,
        trainerProjectDir: 'D:\\Turing\\Projects\\workspace\\llm-trainer-project',
      },
      {
        publishDependencies: {
          fetchImpl: async (url, init) => {
            requests.push({ url, init });
            return {
              ok: true,
              status: 200,
              async json() {
                return { data: { reviewPromptTurn: { id: 'turn-1' } } };
              },
            };
          },
        },
      },
    );

    assert.equal(result.success, true);
    assert.deepEqual(result.command, ['native-publish']);
    assert.equal(requests.length, 1);
    assert.equal(requests[0].url, 'https://rlhf-api.turing.com/graphql');
    assert.match(String(requests[0].init.headers.Cookie), /cookie=value/);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
