import { mkdtemp, mkdir, rm, writeFile } from 'node:fs/promises';
import os from 'node:os';
import { join } from 'node:path';
import assert from 'node:assert/strict';
import test from 'node:test';

import { summarizeResults, VALIDATOR_NAMES } from './common.mjs';
import { buildValidationChecklist } from './checklist.mjs';
import { runPlsqlProgramValidator } from './plsql-program-validator.mjs';

test('runPlsqlProgramValidator emits PASS artifact checks for native analyzer files', async () => {
  const root = await mkdtemp(join(os.tmpdir(), 'app-validator-plsql-program-'));
  const taskDir = join(root, '25002');
  const metadata = {
    id: 25002,
    num_turns: 1,
    required_anonymous_block: false,
    required_procs_funcs_pkgs: false,
    target_reasoning_types: [],
  };

  try {
    await mkdir(taskDir, { recursive: true });
    await writeFile(join(taskDir, '25002_turn1_1user.txt'), 'Requirements:\nAnonymous Block:\n', 'utf8');
    await writeFile(
      join(taskDir, '25002_turn1_4referenceAnswer.sql'),
      'DECLARE\nBEGIN\n  NULL;\nEXCEPTION\n  WHEN OTHERS THEN\n    NULL;\nEND;\n/',
      'utf8',
    );
    await writeFile(join(taskDir, '25002_turn1_3columns.txt'), 'SAMPLE.EMPLOYEES.employee_id\n', 'utf8');
    await writeFile(join(taskDir, '25002_turn1_5testCases.sql'), 'execution_result:\n1\n', 'utf8');
    await writeFile(join(taskDir, '25002_turn1_6reasoningTypes.txt'), 'Exception Handling\n', 'utf8');
    await writeFile(join(taskDir, '25002_turn1_7plSqlConstructs.txt'), 'DECLARE ... BEGIN ... END\n', 'utf8');

    const results = await runPlsqlProgramValidator('25002', taskDir, metadata);
    const expectedArtifacts = [
      ['Columns Artifact', '25002_turn1_3columns.txt'],
      ['Test Cases Artifact', '25002_turn1_5testCases.sql'],
      ['Reasoning Types Artifact', '25002_turn1_6reasoningTypes.txt'],
      ['PL/SQL Constructs Artifact', '25002_turn1_7plSqlConstructs.txt'],
    ];

    for (const [item, sourceFile] of expectedArtifacts) {
      const row = results.find((entry) => entry.item === item);
      assert.ok(row, `missing validation row for ${item}`);
      assert.equal(row.status, 'PASS');
      assert.equal(row.ruleId, 'artifact_present');
      assert.equal(row.sourceFile, sourceFile);
    }

    const checklist = buildValidationChecklist([
      {
        validator: VALIDATOR_NAMES.plsqlProgram,
        results,
        summary: summarizeResults(results),
      },
    ]);

    for (const [item] of expectedArtifacts) {
      const row = checklist.find((entry) => entry.validator === VALIDATOR_NAMES.plsqlProgram && entry.item === item);
      assert.ok(row, `missing checklist row for ${item}`);
      assert.equal(row.status, 'PASS');
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});


