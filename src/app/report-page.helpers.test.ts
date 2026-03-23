import assert from 'node:assert/strict';
import test from 'node:test';

import {
  buildCombinedTaskReviewSections,
  buildCombinedTurnReviewSection,
  buildFileGroups,
  buildValidationStatusCache,
  classifyCanonicalArtifact,
  filterChecklistRows,
  findFileValidationEntry,
  matchesCurrentFile,
  resolveRecalculationFilesForIssue,
  resolveReportFileName,
  sortByCanonicalArtifactOrder,
} from './report-page.helpers';
import { TaskReport, ValidationMasterReport } from './models';

const report: TaskReport = {
  taskId: '9418',
  folderPath: 'D:/task-output/9418',
  files: [
    { name: '9418_turn1_1user.txt', size: 10, modifiedAt: '2026-03-22T10:00:00.000Z', extension: 'txt' },
    { name: '9418_turn1_4referenceAnswer.sql', size: 20, modifiedAt: '2026-03-22T10:00:00.000Z', extension: 'sql' },
    { name: '9418_turn1_5testCases.sql', size: 18, modifiedAt: '2026-03-22T10:00:00.000Z', extension: 'sql' },
    { name: '_logs/validate-2026.log', size: 20, modifiedAt: '2026-03-22T10:00:00.000Z', extension: 'log' },
    { name: '_audit/9418_turn1_6reasoningTypes.audit.json', size: 20, modifiedAt: '2026-03-22T10:00:00.000Z', extension: 'json' },
    { name: '_validation/master_validator_task_9418.json', size: 20, modifiedAt: '2026-03-22T10:00:00.000Z', extension: 'json' },
    { name: '_validation/files/referenceanswer__abcd1234.json', size: 20, modifiedAt: '2026-03-22T10:00:00.000Z', extension: 'json' },
  ],
};

const validationReport: ValidationMasterReport = {
  validator: 'MasterValidator',
  generatedAt: '2026-03-22T10:00:00.000Z',
  taskId: '9418',
  summary: {
    validatorsRun: 1,
    validatorsPassed: 0,
    validatorsFailed: 1,
    itemsTotal: 3,
    itemsPassed: 1,
    itemsFailed: 2,
    taskFailHits: 1,
    tasksTotal: 1,
    tasksPassed: 0,
    tasksFailed: 1,
  },
  validators: [
    {
      validator: 'ArtifactAlignmentValidator',
      success: false,
      summary: { total: 3, passed: 1, failed: 2, tasksTotal: 1, tasksPassed: 0, tasksFailed: 1 },
      reportFile: '_validation/artifactalignment_task_9418.json',
    },
  ],
  checklistCatalog: [],
  checklist: [
    {
      checkId: 'alignment.output',
      category: 'artifact-alignment',
      validator: 'ArtifactAlignmentValidator',
      item: 'Output Literal Test Coverage: ABCD',
      ruleId: 'missing_output_literal_in_testcase',
      description: 'Prompt output must be exercised by the testcase.',
      status: 'FAIL',
      taskId: '9418',
      turnId: 1,
      expected: 'execution_result includes ABCD',
      present: 'execution_result does not include ABCD',
      update: 'Add testcase coverage for ABCD.',
      sourceFile: '9418_turn1_5testCases.sql',
      line: 9,
    },
    {
      checkId: 'alignment.program',
      category: 'artifact-alignment',
      validator: 'ArtifactAlignmentValidator',
      item: 'Required Program Implementation: PROCEDURE sp_emit_message',
      ruleId: 'program_implemented',
      description: 'Prompt-required program exists in the reference answer.',
      status: 'PASS',
      taskId: '9418',
      turnId: 1,
      expected: 'Reference answer implements PROCEDURE sp_emit_message',
      present: 'PROCEDURE sp_emit_message found',
      update: null,
      sourceFile: '9418_turn1_4referenceAnswer.sql',
      line: 1,
    },
    {
      checkId: 'alignment.task',
      category: 'artifact-alignment',
      validator: 'ArtifactAlignmentValidator',
      item: 'Prompt testcase artifact exists',
      ruleId: null,
      description: 'A testcase file exists for the turn.',
      status: 'NOT_RUN',
      taskId: '9418',
      turnId: 1,
      expected: null,
      present: null,
      update: null,
      sourceFile: null,
      line: null,
    },
  ],
  results: [
    {
      taskId: '9418',
      turnId: 1,
      validator: 'ArtifactAlignmentValidator',
      item: 'Required Program Implementation: PROCEDURE sp_emit_message',
      ruleId: 'program_implemented',
      status: 'PASS',
      expected: null,
      present: 'PROCEDURE sp_emit_message found',
      update: null,
      sourceFile: '9418_turn1_4referenceAnswer.sql',
      line: 1,
    },
    {
      taskId: '9418',
      turnId: 1,
      validator: 'ArtifactAlignmentValidator',
      item: 'Output Literal Test Coverage: ABCD',
      ruleId: 'missing_output_literal_in_testcase',
      status: 'FAIL',
      expected: 'execution_result includes ABCD',
      present: 'execution_result does not include ABCD',
      update: 'Add testcase coverage for ABCD.',
      sourceFile: '9418_turn1_5testCases.sql',
      line: 9,
    },
    {
      taskId: '9418',
      turnId: 1,
      validator: 'ArtifactAlignmentValidator',
      item: 'Exception Test Coverage: Unexpected error occurred',
      ruleId: 'missing_exception_literal_in_testcase',
      status: 'FAIL',
      expected: 'execution_result includes Unexpected error occurred',
      present: 'execution_result does not include Unexpected error occurred',
      update: 'Add testcase coverage for Unexpected error occurred.',
      sourceFile: '9418_turn1_5testCases.sql',
      line: 12,
    },
  ],
  fileReports: [
    {
      sourceFile: '9418_turn1_4referenceAnswer.sql',
      turnId: 1,
      reportFile: '_validation/files/referenceanswer__abcd1234.json',
      logFile: '_validation/files/referenceanswer__abcd1234.log',
      summary: { total: 1, passed: 1, failed: 0, tasksTotal: 1, tasksPassed: 1, tasksFailed: 0 },
      validators: [
        {
          validator: 'ArtifactAlignmentValidator',
          success: true,
          summary: { total: 1, passed: 1, failed: 0, tasksTotal: 1, tasksPassed: 1, tasksFailed: 0 },
        },
      ],
    },
    {
      sourceFile: '9418_turn1_5testCases.sql',
      turnId: 1,
      reportFile: '_validation/files/testcases__ef567890.json',
      logFile: '_validation/files/testcases__ef567890.log',
      summary: { total: 2, passed: 0, failed: 2, tasksTotal: 1, tasksPassed: 0, tasksFailed: 1 },
      validators: [
        {
          validator: 'ArtifactAlignmentValidator',
          success: false,
          summary: { total: 2, passed: 0, failed: 2, tasksTotal: 1, tasksPassed: 0, tasksFailed: 1 },
        },
      ],
    },
  ],
};

test('buildFileGroups hides internal validation sidecars and keeps task logs and audits out of turn groups', () => {
  const groups = buildFileGroups(report);

  assert.equal(groups.some((group) => group.files.includes('_validation/files/referenceanswer__abcd1234.json')), false);
  assert.deepEqual(groups.find((group) => group.key === 'turn-1')?.files, [
    '9418_turn1_1user.txt',
    '9418_turn1_4referenceAnswer.sql',
    '9418_turn1_5testCases.sql',
  ]);
  assert.deepEqual(groups.find((group) => group.key === 'logs')?.files, ['_logs/validate-2026.log']);
  assert.deepEqual(groups.find((group) => group.key === 'audit')?.files, ['_audit/9418_turn1_6reasoningTypes.audit.json']);
  assert.deepEqual(groups.find((group) => group.key === 'validation')?.files, ['_validation/master_validator_task_9418.json']);
});

test('resolveReportFileName and matchesCurrentFile support basename matching', () => {
  assert.equal(resolveReportFileName(report.files, 'folder/9418_turn1_4referenceAnswer.sql'), '9418_turn1_4referenceAnswer.sql');
  assert.equal(
    matchesCurrentFile('9418_turn1_4referenceAnswer.sql', report.files, 'nested/path/9418_turn1_4referenceAnswer.sql'),
    true,
  );
});

test('buildValidationStatusCache marks failing files and turns while preserving passing files', () => {
  const cache = buildValidationStatusCache(validationReport, report.files);

  assert.equal(cache.fileStates.get('9418_turn1_4referenceAnswer.sql'), 'success');
  assert.equal(cache.fileStates.get('9418_turn1_5testCases.sql'), 'fail');
  assert.equal(cache.turnStates.get('turn-1'), 'fail');
});

test('findFileValidationEntry returns the sidecar entry for the selected file', () => {
  const entry = findFileValidationEntry(validationReport, report.files, '9418_turn1_5testCases.sql');

  assert.equal(entry?.reportFile, '_validation/files/testcases__ef567890.json');
  assert.equal(entry?.logFile, '_validation/files/testcases__ef567890.log');
});

test('filterChecklistRows supports current-file filtering and failure-only mode', () => {
  const currentFileFailures = filterChecklistRows(
    validationReport.checklist,
    report.files,
    '9418_turn1_5testCases.sql',
    true,
    false,
  );
  assert.equal(currentFileFailures.length, 1);
  assert.equal(currentFileFailures[0]?.item, 'Output Literal Test Coverage: ABCD');

  const allFailures = filterChecklistRows(validationReport.checklist, report.files, '', false, true);
  assert.equal(allFailures.length, 1);
  assert.equal(allFailures[0]?.status, 'FAIL');
});


test('resolveRecalculationFilesForIssue returns all turn files for complexity table count mismatches', () => {
  const files = resolveRecalculationFilesForIssue(report, {
    validator: 'ComplexityTableCountValidator',
    item: 'Complexity Table Count',
    ruleId: 'count_mismatch',
    turnId: 1,
    sourceFile: '9418_turn1_4referenceAnswer.sql',
  });

  assert.deepEqual(files, [
    '9418_turn1_1user.txt',
    '9418_turn1_4referenceAnswer.sql',
    '9418_turn1_5testCases.sql',
  ]);
});

test('resolveRecalculationFilesForIssue ignores unrelated validation failures', () => {
  const files = resolveRecalculationFilesForIssue(report, {
    validator: 'ArtifactAlignmentValidator',
    item: 'Output Literal Test Coverage: ABCD',
    ruleId: 'missing_output_literal_in_testcase',
    turnId: 1,
    sourceFile: '9418_turn1_5testCases.sql',
  });

  assert.deepEqual(files, []);
});


const combinedReviewReport: TaskReport = {
  taskId: '9418',
  folderPath: 'D:/task-output/9418',
  files: [
    { name: '9418_turn2_5testCases.sql', size: 18, modifiedAt: '2026-03-22T10:00:00.000Z', extension: 'sql' },
    { name: '9418_turn1_4referenceAnswer.sql', size: 20, modifiedAt: '2026-03-22T10:00:00.000Z', extension: 'sql' },
    { name: '9418_turn1_1user.txt', size: 10, modifiedAt: '2026-03-22T10:00:00.000Z', extension: 'txt' },
    { name: '9418_turn1_7plSqlConstructs.txt', size: 12, modifiedAt: '2026-03-22T10:00:00.000Z', extension: 'txt' },
    { name: '9418_turn1_2tables.txt', size: 10, modifiedAt: '2026-03-22T10:00:00.000Z', extension: 'txt' },
    { name: '9418_turn1_3columns.txt', size: 10, modifiedAt: '2026-03-22T10:00:00.000Z', extension: 'txt' },
    { name: '9418_turn1_6reasoningTypes.txt', size: 12, modifiedAt: '2026-03-22T10:00:00.000Z', extension: 'txt' },
    { name: '9418_turn1_5testCases.sql', size: 18, modifiedAt: '2026-03-22T10:00:00.000Z', extension: 'sql' },
    { name: '9418_turn2_1user.txt', size: 10, modifiedAt: '2026-03-22T10:00:00.000Z', extension: 'txt' },
    { name: '9418_turn2_4referenceAnswer.sql', size: 20, modifiedAt: '2026-03-22T10:00:00.000Z', extension: 'sql' },
    { name: '9418_turn2_9notes.txt', size: 10, modifiedAt: '2026-03-22T10:00:00.000Z', extension: 'txt' },
    { name: '9418_1metadata.json', size: 30, modifiedAt: '2026-03-22T10:00:00.000Z', extension: 'json' },
    { name: '_logs/validate-2026.log', size: 20, modifiedAt: '2026-03-22T10:00:00.000Z', extension: 'log' },
    { name: '_audit/9418_turn1_6reasoningTypes.audit.json', size: 20, modifiedAt: '2026-03-22T10:00:00.000Z', extension: 'json' },
    { name: '_validation/master_validator_task_9418.json', size: 20, modifiedAt: '2026-03-22T10:00:00.000Z', extension: 'json' },
    { name: '_validation/files/referenceanswer__abcd1234.json', size: 20, modifiedAt: '2026-03-22T10:00:00.000Z', extension: 'json' },
  ],
};

test('classifyCanonicalArtifact recognizes the main turn files only', () => {
  assert.equal(classifyCanonicalArtifact('9418', '9418_turn1_1user.txt'), 'prompt');
  assert.equal(classifyCanonicalArtifact('9418', '9418_turn1_2tables.txt'), 'tables');
  assert.equal(classifyCanonicalArtifact('9418', '9418_turn1_3columns.txt'), 'columns');
  assert.equal(classifyCanonicalArtifact('9418', '9418_turn1_4referenceAnswer.sql'), 'plsql-program');
  assert.equal(classifyCanonicalArtifact('9418', '9418_turn1_5testCases.sql'), 'test-cases');
  assert.equal(classifyCanonicalArtifact('9418', '9418_turn1_6reasoningTypes.txt'), 'reasoning-types');
  assert.equal(classifyCanonicalArtifact('9418', '9418_turn1_7plSqlConstructs.txt'), 'plsql-constructors');
  assert.equal(classifyCanonicalArtifact('9418', '_logs/validate-2026.log'), null);
  assert.equal(classifyCanonicalArtifact('9418', '9418_1metadata.json'), null);
});

test('sortByCanonicalArtifactOrder keeps the canonical turn artifact sequence', () => {
  assert.deepEqual(sortByCanonicalArtifactOrder('9418', [
    '9418_turn1_5testCases.sql',
    '9418_turn1_2tables.txt',
    '9418_turn1_7plSqlConstructs.txt',
    '9418_turn1_1user.txt',
    '9418_turn1_4referenceAnswer.sql',
    '9418_turn1_6reasoningTypes.txt',
    '9418_turn1_3columns.txt',
  ]), [
    '9418_turn1_1user.txt',
    '9418_turn1_2tables.txt',
    '9418_turn1_3columns.txt',
    '9418_turn1_4referenceAnswer.sql',
    '9418_turn1_5testCases.sql',
    '9418_turn1_6reasoningTypes.txt',
    '9418_turn1_7plSqlConstructs.txt',
  ]);
});

test('buildCombinedTurnReviewSection returns only recognized main files in canonical order', () => {
  const section = buildCombinedTurnReviewSection(combinedReviewReport, 1);

  assert.equal(section?.turnId, 1);
  assert.deepEqual(section?.files.map((file) => file.name), [
    '9418_turn1_1user.txt',
    '9418_turn1_2tables.txt',
    '9418_turn1_3columns.txt',
    '9418_turn1_4referenceAnswer.sql',
    '9418_turn1_5testCases.sql',
    '9418_turn1_6reasoningTypes.txt',
    '9418_turn1_7plSqlConstructs.txt',
  ]);
});

test('buildCombinedTaskReviewSections returns each turn in numeric order and omits unknown artifacts', () => {
  const sections = buildCombinedTaskReviewSections(combinedReviewReport);

  assert.deepEqual(sections.map((section) => section.turnId), [1, 2]);
  assert.deepEqual(sections[0]?.files.map((file) => file.name), [
    '9418_turn1_1user.txt',
    '9418_turn1_2tables.txt',
    '9418_turn1_3columns.txt',
    '9418_turn1_4referenceAnswer.sql',
    '9418_turn1_5testCases.sql',
    '9418_turn1_6reasoningTypes.txt',
    '9418_turn1_7plSqlConstructs.txt',
  ]);
  assert.deepEqual(sections[1]?.files.map((file) => file.name), [
    '9418_turn2_1user.txt',
    '9418_turn2_4referenceAnswer.sql',
    '9418_turn2_5testCases.sql',
  ]);
});
