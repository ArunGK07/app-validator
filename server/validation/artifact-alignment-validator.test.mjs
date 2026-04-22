import { mkdtemp, mkdir, rm, writeFile } from 'node:fs/promises';
import os from 'node:os';
import { join } from 'node:path';
import assert from 'node:assert/strict';
import test from 'node:test';

import { runArtifactAlignmentValidator } from './artifact-alignment-validator.mjs';

test('runArtifactAlignmentValidator passes when prompt names and literal coverage align across code and testcases', async () => {
  const root = await mkdtemp(join(os.tmpdir(), 'app-validator-artifact-alignment-'));
  const taskDir = join(root, '31001');
  const metadata = { id: 31001, num_turns: 1 };

  try {
    await mkdir(taskDir, { recursive: true });
    await writeFile(
      join(taskDir, '31001_turn1_1user.txt'),
      [
        'Requirements:',
        'Procedure Name:',
        'sp_emit_message',
        '',
        'Parameters:',
        '\tp_input - IN - NUMBER -- sample',
        '',
        'Output:',
        '\tMessage: ABCD',
        '',
        'Exception Handling:',
        '\tOther Exception : Unexpected error occurred',
      ].join('\n'),
      'utf8',
    );
    await writeFile(
      join(taskDir, '31001_turn1_4referenceAnswer.sql'),
      [
        'CREATE OR REPLACE PROCEDURE sp_emit_message IS',
        'BEGIN',
        "  DBMS_OUTPUT.PUT_LINE('ABCD');",
        'EXCEPTION',
        '  WHEN OTHERS THEN',
        "    DBMS_OUTPUT.PUT_LINE('Unexpected error occurred');",
        'END;',
        '/',
      ].join('\n'),
      'utf8',
    );
    await writeFile(
      join(taskDir, '31001_turn1_5testCases.sql'),
      [
        'Test Case 1:',
        'execution_instructions:',
        'BEGIN sp_emit_message; END;',
        'execution_result:',
        'ABCD',
        '',
      ].join('\n'),
      'utf8',
    );

    const results = await runArtifactAlignmentValidator('31001', taskDir, metadata);

    assert.ok(results.some((entry) => entry.ruleId === 'program_implemented' && entry.status === 'PASS'));
    assert.ok(results.some((entry) => entry.ruleId === 'testcase_program_covered' && entry.status === 'PASS'));
    assert.ok(results.some((entry) => entry.ruleId === 'output_literal_in_code' && entry.status === 'PASS'));
    assert.ok(results.some((entry) => entry.ruleId === 'output_literal_in_testcase' && entry.status === 'PASS'));
    assert.ok(results.some((entry) => entry.ruleId === 'exception_message_in_code' && entry.status === 'PASS'));
    assert.ok(results.some((entry) => entry.ruleId === 'testcase_coverage_not_required' && entry.status === 'PASS'));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test('runArtifactAlignmentValidator fails on missing program implementation and testcase coverage', async () => {
  const root = await mkdtemp(join(os.tmpdir(), 'app-validator-artifact-alignment-'));
  const taskDir = join(root, '31002');
  const metadata = { id: 31002, num_turns: 1 };

  try {
    await mkdir(taskDir, { recursive: true });
    await writeFile(
      join(taskDir, '31002_turn1_1user.txt'),
      [
        'Requirements:',
        'Procedure Name:',
        'sp_expected_name',
        '',
        'Parameters:',
        '\tp_input - IN - NUMBER -- sample',
        '',
        'Output:',
        '\tMessage: ABCD',
        '',
        'Exception Handling:',
        '\tOther Exception : Unexpected error occurred',
      ].join('\n'),
      'utf8',
    );
    await writeFile(
      join(taskDir, '31002_turn1_4referenceAnswer.sql'),
      [
        'CREATE OR REPLACE PROCEDURE sp_other_name IS',
        'BEGIN',
        '  NULL;',
        'END;',
        '/',
      ].join('\n'),
      'utf8',
    );
    await writeFile(
      join(taskDir, '31002_turn1_5testCases.sql'),
      [
        'Test Case 1:',
        'execution_instructions:',
        'BEGIN sp_other_name; END;',
        'execution_result:',
        'nothing happened',
        '',
      ].join('\n'),
      'utf8',
    );

    const results = await runArtifactAlignmentValidator('31002', taskDir, metadata);

    assert.ok(results.some((entry) => entry.ruleId === 'missing_program_implementation' && entry.status === 'FAIL'));
    assert.ok(results.some((entry) => entry.ruleId === 'missing_testcase_program_coverage' && entry.status === 'FAIL'));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test('runArtifactAlignmentValidator enforces stable label fragments from placeholder-based prompt lines', async () => {
  const root = await mkdtemp(join(os.tmpdir(), 'app-validator-artifact-alignment-'));
  const taskDir = join(root, '31003');
  const metadata = { id: 31003, num_turns: 1 };

  try {
    await mkdir(taskDir, { recursive: true });
    await writeFile(
      join(taskDir, '31003_turn1_1user.txt'),
      [
        'Requirements:',
        'Procedure Name:',
        'sp_emit_message',
        '',
        'Parameters:',
        '\tp_input - IN - NUMBER -- sample',
        '',
        'Output:',
        '\tMessage: ABCD',
        '\tCountry Code: [country_code]',
        '',
        'Exception Handling:',
        '\tOther Exception : Unexpected error occurred',
      ].join('\n'),
      'utf8',
    );
    await writeFile(
      join(taskDir, '31003_turn1_4referenceAnswer.sql'),
      [
        'CREATE OR REPLACE PROCEDURE sp_emit_message IS',
        'BEGIN',
        "  DBMS_OUTPUT.PUT_LINE('ABCD');",
        'EXCEPTION',
        '  WHEN OTHERS THEN',
        '    NULL;',
        'END;',
        '/',
      ].join('\n'),
      'utf8',
    );
    await writeFile(
      join(taskDir, '31003_turn1_5testCases.sql'),
      [
        'Test Case 1:',
        'execution_instructions:',
        'BEGIN sp_emit_message; END;',
        'execution_result:',
        'not the expected output',
        '',
      ].join('\n'),
      'utf8',
    );

    const results = await runArtifactAlignmentValidator('31003', taskDir, metadata);

    assert.ok(results.some((entry) => entry.ruleId === 'missing_output_literal_in_testcase' && entry.status === 'FAIL'));
    assert.ok(results.some((entry) => entry.item === 'Output Literal Code Coverage: Country Code: ' && entry.status === 'FAIL'));
    assert.ok(results.some((entry) => entry.item === 'Output Literal Test Coverage: Country Code: ' && entry.status === 'FAIL'));
    assert.ok(results.some((entry) => entry.ruleId === 'missing_exception_message_in_code' && entry.status === 'FAIL'));
    assert.ok(results.some((entry) => entry.ruleId === 'testcase_coverage_not_required' && entry.status === 'PASS'));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test('runArtifactAlignmentValidator recognizes procedures implemented inside a package body', async () => {
  const root = await mkdtemp(join(os.tmpdir(), 'app-validator-artifact-alignment-'));
  const taskDir = join(root, '31004');
  const metadata = { id: 31004, num_turns: 1 };

  try {
    await mkdir(taskDir, { recursive: true });
    await writeFile(
      join(taskDir, '31004_turn1_1user.txt'),
      [
        'Requirements:',
        'Package Name:',
        'pkg_demo',
        'Procedure Name:',
        'sp_emit_message',
        '',
        'Parameters:',
        'sp_emit_message:',
        '\tp_input - IN - NUMBER -- sample',
        '',
        'Output:',
        'sp_emit_message:',
        '\tMessage: ABCD',
        '',
        'Exception Handling:',
        'sp_emit_message:',
        '\tOther Exception : Unexpected error occurred',
      ].join('\n'),
      'utf8',
    );
    await writeFile(
      join(taskDir, '31004_turn1_4referenceAnswer.sql'),
      [
        'CREATE OR REPLACE PACKAGE pkg_demo IS',
        '  PROCEDURE sp_emit_message(p_input IN NUMBER);',
        'END pkg_demo;',
        '/',
        'CREATE OR REPLACE PACKAGE BODY pkg_demo IS',
        '  PROCEDURE sp_emit_message(p_input IN NUMBER) IS',
        '  BEGIN',
        "    DBMS_OUTPUT.PUT_LINE('ABCD');",
        '  EXCEPTION',
        '    WHEN OTHERS THEN',
        "      DBMS_OUTPUT.PUT_LINE('Unexpected error occurred');",
        '  END sp_emit_message;',
        'END pkg_demo;',
        '/',
      ].join('\n'),
      'utf8',
    );
    await writeFile(
      join(taskDir, '31004_turn1_5testCases.sql'),
      [
        'Test Case 1:',
        'execution_instructions:',
        'BEGIN pkg_demo.sp_emit_message(1); END;',
        'execution_result:',
        'ABCD',
        '',
      ].join('\n'),
      'utf8',
    );

    const results = await runArtifactAlignmentValidator('31004', taskDir, metadata);

    assert.ok(results.some((entry) => entry.item === 'Required Program Implementation: PACKAGE pkg_demo' && entry.status === 'PASS'));
    assert.ok(results.some((entry) => entry.item === 'Required Program Implementation: PROCEDURE sp_emit_message' && entry.status === 'PASS'));
    assert.ok(!results.some((entry) => entry.ruleId === 'missing_program_implementation' && entry.status === 'FAIL'));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test('runArtifactAlignmentValidator fails when exception message is only a substring of a dynamic handler output', async () => {
  const root = await mkdtemp(join(os.tmpdir(), 'app-validator-artifact-alignment-'));
  const taskDir = join(root, '31005');
  const metadata = { id: 31005, num_turns: 1 };

  try {
    await mkdir(taskDir, { recursive: true });
    await writeFile(
      join(taskDir, '31005_turn1_1user.txt'),
      [
        'Requirements:',
        'Procedure Name:',
        'sp_emit_message',
        '',
        'Parameters:',
        '\tp_input - IN - NUMBER -- sample',
        '',
        'Output:',
        '\tMessage: ABCD',
        '',
        'Exception Handling:',
        '\tOther Exception : Unexpected error occurred',
      ].join('\n'),
      'utf8',
    );
    await writeFile(
      join(taskDir, '31005_turn1_4referenceAnswer.sql'),
      [
        'CREATE OR REPLACE PROCEDURE sp_emit_message(p_input IN NUMBER) IS',
        'BEGIN',
        "  DBMS_OUTPUT.PUT_LINE('ABCD');",
        'EXCEPTION',
        '  WHEN OTHERS THEN',
        "    DBMS_OUTPUT.PUT_LINE('Unexpected error occurred while auditing class ID ' || p_input || '.');",
        'END;',
        '/',
      ].join('\n'),
      'utf8',
    );
    await writeFile(
      join(taskDir, '31005_turn1_5testCases.sql'),
      [
        'Test Case 1:',
        'execution_instructions:',
        'BEGIN sp_emit_message(1); END;',
        'execution_result:',
        'ABCD',
        '',
      ].join('\n'),
      'utf8',
    );

    const results = await runArtifactAlignmentValidator('31005', taskDir, metadata);

    assert.ok(results.some((entry) => entry.ruleId === 'missing_exception_message_in_code' && entry.status === 'FAIL'));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test('runArtifactAlignmentValidator derives placeholder-based exception output fragments instead of narrative prompt text', async () => {
  const root = await mkdtemp(join(os.tmpdir(), 'app-validator-artifact-alignment-'));
  const taskDir = join(root, '31006');
  const metadata = { id: 31006, num_turns: 1 };

  try {
    await mkdir(taskDir, { recursive: true });
    await writeFile(
      join(taskDir, '31006_turn1_1user.txt'),
      [
        'Requirements:',
        'Procedure Name:',
        'sp_emit_message',
        '',
        'Parameters:',
        '\tp_input - IN - NUMBER -- sample',
        '',
        'Output:',
        '\tMessage: ABCD',
        '',
        'Exception Handling:',
        '\tProduct Not Found : If a purchase references a missing product then print SKIPPED : Purchase ID <purchase_id> - Product not found.',
        '\tOther Exception : Unexpected error occurred.',
      ].join('\n'),
      'utf8',
    );
    await writeFile(
      join(taskDir, '31006_turn1_4referenceAnswer.sql'),
      [
        'CREATE OR REPLACE PROCEDURE sp_emit_message(p_input IN NUMBER) IS',
        'BEGIN',
        "  DBMS_OUTPUT.PUT_LINE('ABCD');",
        'EXCEPTION',
        '  WHEN NO_DATA_FOUND THEN',
        "    DBMS_OUTPUT.PUT_LINE('SKIPPED: Purchase ID ' || p_input || ' - Product not found.');",
        '  WHEN OTHERS THEN',
        "    DBMS_OUTPUT.PUT_LINE('Unexpected error occurred.');",
        'END;',
        '/',
      ].join('\n'),
      'utf8',
    );
    await writeFile(
      join(taskDir, '31006_turn1_5testCases.sql'),
      [
        'Test Case 1:',
        'execution_instructions:',
        'BEGIN sp_emit_message(999001); END;',
        'execution_result:',
        'ABCD',
        'SKIPPED: Purchase ID 999001 - Product not found.',
        '',
      ].join('\n'),
      'utf8',
    );

    const results = await runArtifactAlignmentValidator('31006', taskDir, metadata);

    assert.ok(results.some((entry) => entry.item === 'Exception Message Code Coverage: SKIPPED: Purchase ID' && entry.status === 'PASS'));
    assert.ok(results.some((entry) => entry.item === 'Exception Message Code Coverage: - Product not found.' && entry.status === 'PASS'));
    assert.ok(results.some((entry) => entry.item === 'Exception Message Code Coverage: Unexpected error occurred.' && entry.status === 'PASS'));
    assert.ok(results.some((entry) => entry.item === 'Exception Message Test Coverage: Unexpected error occurred.' && entry.ruleId === 'testcase_coverage_not_required' && entry.status === 'PASS'));
    assert.ok(!results.some((entry) => entry.item.includes('If a purchase references a missing product then print') && entry.status === 'FAIL'));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test('runArtifactAlignmentValidator ignores instructional output prose and keeps only concrete placeholder fragments', async () => {
  const root = await mkdtemp(join(os.tmpdir(), 'app-validator-artifact-alignment-'));
  const taskDir = join(root, '31007');
  const metadata = { id: 31007, num_turns: 1 };

  try {
    await mkdir(taskDir, { recursive: true });
    await writeFile(
      join(taskDir, '31007_turn1_1user.txt'),
      [
        'Requirements:',
        'Trigger Name:',
        'trg_demo',
        '',
        'Parameters:',
        '\tNo parameters required.',
        '',
        'Output:',
        '\tPrint audit report rows for logged warehouse activity.',
        '\tFormat reorder trigger log messages as REORDER ALERT: Product <product_id> | <status> and do not include exception backtraces or call-stack text in normal logging.',
        '\tSort audit report rows by log_time in descending order. No ranking rules are required.',
        '',
        'Exception Handling:',
        '\tOther Exception : Unexpected error occurred.',
      ].join('\n'),
      'utf8',
    );
    await writeFile(
      join(taskDir, '31007_turn1_4referenceAnswer.sql'),
      [
        'CREATE OR REPLACE TRIGGER trg_demo',
        'AFTER INSERT ON demo_table',
        'FOR EACH ROW',
        'BEGIN',
        "  DBMS_OUTPUT.PUT_LINE('REORDER ALERT: Product ' || :NEW.product_id || ' | WARNING');",
        'EXCEPTION',
        '  WHEN OTHERS THEN',
        "    DBMS_OUTPUT.PUT_LINE('Unexpected error occurred.');",
        'END;',
        '/',
      ].join('\n'),
      'utf8',
    );
    await writeFile(
      join(taskDir, '31007_turn1_5testCases.sql'),
      [
        'Test Case 1:',
        'execution_instructions:',
        'UPDATE demo_table SET qty = qty WHERE id = 1;',
        'execution_result:',
        'REORDER ALERT: Product 1 | WARNING',
        '',
      ].join('\n'),
      'utf8',
    );

    const results = await runArtifactAlignmentValidator('31007', taskDir, metadata);

    assert.ok(results.some((entry) => entry.item === 'Output Literal Code Coverage: REORDER ALERT: Product' && entry.status === 'PASS'));
    assert.ok(!results.some((entry) => entry.item.includes('Print audit report rows for logged warehouse activity') && entry.status === 'FAIL'));
    assert.ok(!results.some((entry) => entry.item.includes('Sort audit report rows by log_time in descending order') && entry.status === 'FAIL'));
    assert.ok(!results.some((entry) => entry.item.includes('and do not include exception backtraces or call-stack text in normal logging') && entry.status === 'FAIL'));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
