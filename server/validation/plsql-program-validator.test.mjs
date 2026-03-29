import { mkdtemp, mkdir, rm, writeFile } from 'node:fs/promises';
import os from 'node:os';
import { join } from 'node:path';
import assert from 'node:assert/strict';
import test from 'node:test';

import { summarizeResults, VALIDATOR_NAMES } from './common.mjs';
import { buildValidationChecklist } from './checklist.mjs';
import { runPlsqlProgramValidator } from './plsql-program-validator.mjs';
import { analyzeConstructs, analyzeReasoningTypes, formatCommaLines } from '../generation/analyzers.mjs';

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
    const codeText = 'DECLARE\nBEGIN\n  NULL;\nEXCEPTION\n  WHEN OTHERS THEN\n    NULL;\nEND;\n/';
    await writeFile(
      join(taskDir, '25002_turn1_4referenceAnswer.sql'),
      codeText,
      'utf8',
    );
    await writeFile(join(taskDir, '25002_turn1_3columns.txt'), 'SAMPLE.EMPLOYEES.employee_id\n', 'utf8');
    await writeFile(join(taskDir, '25002_turn1_5testCases.sql'), 'execution_result:\n1\n', 'utf8');
    await writeFile(join(taskDir, '25002_turn1_6reasoningTypes.txt'), `${formatCommaLines(analyzeReasoningTypes(codeText))}\n`, 'utf8');
    await writeFile(join(taskDir, '25002_turn1_7plSqlConstructs.txt'), `${formatCommaLines(analyzeConstructs(codeText))}\n`, 'utf8');

    const results = await runPlsqlProgramValidator('25002', taskDir, metadata);
    const expectedArtifacts = [
      ['Columns Artifact', '25002_turn1_3columns.txt'],
      ['Test Cases Artifact', '25002_turn1_5testCases.sql'],
      ['Reasoning Types Artifact', '25002_turn1_6reasoningTypes.txt'],
      ['PL/SQL Constructs Artifact', '25002_turn1_7plSqlConstructs.txt'],
    ];

    const deterministicTimeRow = results.find((entry) => entry.item === 'Non-Deterministic Time Usage');
    assert.ok(deterministicTimeRow, 'missing validation row for Non-Deterministic Time Usage');
    assert.equal(deterministicTimeRow.status, 'PASS');
    assert.equal(deterministicTimeRow.ruleId, 'not_present');

    for (const [item, sourceFile] of expectedArtifacts) {
      const row = results.find((entry) => entry.item === item);
      assert.ok(row, `missing validation row for ${item}`);
      assert.equal(row.status, 'PASS');
      assert.equal(row.ruleId, 'artifact_present');
      assert.equal(row.sourceFile, sourceFile);
    }

    const semanticsRows = [
      ['Reasoning Types Artifact Semantics', '25002_turn1_6reasoningTypes.txt'],
      ['PL/SQL Constructs Artifact Semantics', '25002_turn1_7plSqlConstructs.txt'],
    ];

    for (const [item, sourceFile] of semanticsRows) {
      const row = results.find((entry) => entry.item === item);
      assert.ok(row, `missing validation row for ${item}`);
      assert.equal(row.status, 'PASS');
      assert.equal(row.ruleId, 'artifact_semantics_match');
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

test('runPlsqlProgramValidator fails when generated analyzer artifacts are stale or incorrect', async () => {
  const root = await mkdtemp(join(os.tmpdir(), 'app-validator-plsql-program-artifacts-'));
  const taskDir = join(root, '25004');
  const metadata = {
    id: 25004,
    num_turns: 1,
    required_anonymous_block: false,
    required_procs_funcs_pkgs: false,
    target_reasoning_types: [],
  };

  try {
    await mkdir(taskDir, { recursive: true });
    await writeFile(join(taskDir, '25004_turn1_1user.txt'), 'Requirements:\nAnonymous Block:\n', 'utf8');
    const codeText = [
      'DECLARE',
      '  lv_value NUMBER := 1;',
      'BEGIN',
      '  IF lv_value = 1 THEN',
      "    DBMS_OUTPUT.PUT_LINE('ok');",
      '  END IF;',
      'EXCEPTION',
      '  WHEN OTHERS THEN',
      '    NULL;',
      'END;',
      '/',
    ].join('\n');
    await writeFile(join(taskDir, '25004_turn1_4referenceAnswer.sql'), codeText, 'utf8');
    await writeFile(join(taskDir, '25004_turn1_3columns.txt'), 'SAMPLE.EMPLOYEES.employee_id\n', 'utf8');
    await writeFile(join(taskDir, '25004_turn1_5testCases.sql'), 'execution_result:\n1\n', 'utf8');
    await writeFile(join(taskDir, '25004_turn1_6reasoningTypes.txt'), 'Collections,\nException Handling\n', 'utf8');
    await writeFile(join(taskDir, '25004_turn1_7plSqlConstructs.txt'), 'SELF JOIN,\nDECLARE ... BEGIN ... END\n', 'utf8');

    const results = await runPlsqlProgramValidator('25004', taskDir, metadata);

    const reasoningRow = results.find((entry) => entry.item === 'Reasoning Types Artifact Semantics');
    assert.ok(reasoningRow, 'missing validation row for Reasoning Types Artifact Semantics');
    assert.equal(reasoningRow.status, 'FAIL');
    assert.equal(reasoningRow.ruleId, 'artifact_semantics_mismatch');
    assert.equal(reasoningRow.sourceFile, '25004_turn1_6reasoningTypes.txt');
    assert.match(reasoningRow.present ?? '', /overclaimed: Collections/i);

    const constructsRow = results.find((entry) => entry.item === 'PL\/SQL Constructs Artifact Semantics');
    assert.ok(constructsRow, 'missing validation row for PL/SQL Constructs Artifact Semantics');
    assert.equal(constructsRow.status, 'FAIL');
    assert.equal(constructsRow.ruleId, 'artifact_semantics_mismatch');
    assert.equal(constructsRow.sourceFile, '25004_turn1_7plSqlConstructs.txt');
    assert.match(constructsRow.present ?? '', /overclaimed: SELF JOIN/i);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});



test('runPlsqlProgramValidator fails when reference answers use volatile time sources', async () => {
  const root = await mkdtemp(join(os.tmpdir(), 'app-validator-plsql-program-time-'));
  const taskDir = join(root, '25003');
  const metadata = {
    id: 25003,
    num_turns: 1,
    required_anonymous_block: false,
    required_procs_funcs_pkgs: false,
    target_reasoning_types: [],
  };

  try {
    await mkdir(taskDir, { recursive: true });
    await writeFile(join(taskDir, '25003_turn1_1user.txt'), 'Requirements:\nAnonymous Block:\n', 'utf8');
    await writeFile(
      join(taskDir, '25003_turn1_4referenceAnswer.sql'),
      'DECLARE\n  lv_today DATE := SYSDATE;\nBEGIN\n  DBMS_OUTPUT.PUT_LINE(CURRENT_TIMESTAMP);\nEXCEPTION\n  WHEN OTHERS THEN\n    NULL;\nEND;\n/',
      'utf8',
    );
    await writeFile(join(taskDir, '25003_turn1_3columns.txt'), 'SAMPLE.EMPLOYEES.employee_id\n', 'utf8');
    await writeFile(join(taskDir, '25003_turn1_5testCases.sql'), 'execution_result:\n1\n', 'utf8');
    await writeFile(join(taskDir, '25003_turn1_6reasoningTypes.txt'), 'Exception Handling\n', 'utf8');
    await writeFile(join(taskDir, '25003_turn1_7plSqlConstructs.txt'), 'DECLARE ... BEGIN ... END\n', 'utf8');

    const results = await runPlsqlProgramValidator('25003', taskDir, metadata);
    const row = results.find((entry) => entry.item === 'Non-Deterministic Time Usage');
    assert.ok(row, 'missing validation row for Non-Deterministic Time Usage');
    assert.equal(row.status, 'FAIL');
    assert.equal(row.ruleId, 'disallowed_nondeterministic_time_source');
    assert.equal(row.sourceFile, '25003_turn1_4referenceAnswer.sql');
    assert.equal(row.line, 2);
    assert.match(row.present ?? '', /SYSDATE at line 2/);
    assert.match(row.present ?? '', /CURRENT_TIMESTAMP at line 4/);

    const checklist = buildValidationChecklist([
      {
        validator: VALIDATOR_NAMES.plsqlProgram,
        results,
        summary: summarizeResults(results),
      },
    ]);

    const checklistRow = checklist.find(
      (entry) => entry.validator === VALIDATOR_NAMES.plsqlProgram && entry.item === 'Non-Deterministic Time Usage',
    );
    assert.ok(checklistRow, 'missing checklist row for Non-Deterministic Time Usage');
    assert.equal(checklistRow.status, 'FAIL');
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
