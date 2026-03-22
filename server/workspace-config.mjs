import { resolve as pathResolve } from 'node:path';

export const DEFAULT_TASK_OUTPUT_DIR = pathResolve('D:/Turing/Projects/workspace/task-output');
const DEFAULT_SCHEMA_CACHE_DIR_NAME = 'schema';

const TASK_ARTIFACT_TEMPLATES = {
  schema_file: '{taskId}_0schema.json',
  existing_output_file: '{taskId}_existing_output.json',
  metadata_file: '{taskId}_1metadata.json',
  extracted_value_file_prefix: '{taskId}_turn{turnNumber}_',
  extracted_value_file_suffix: '.txt',
  turn_user_file: '{taskId}_turn{turnNumber}_1user.txt',
  turn_tables_file: '{taskId}_turn{turnNumber}_2tables.txt',
  turn_columns_file: '{taskId}_turn{turnNumber}_3columns.txt',
  turn_reference_answer_file: '{taskId}_turn{turnNumber}_4referenceAnswer.sql',
  turn_test_cases_file: '{taskId}_turn{turnNumber}_5testCases.sql',
  turn_reasoning_types_file: '{taskId}_turn{turnNumber}_6reasoningTypes.txt',
  turn_reasoning_types_audit_file: '_audit/{taskId}_turn{turnNumber}_6reasoningTypes.audit.json',
  turn_plsql_constructs_file: '{taskId}_turn{turnNumber}_7plSqlConstructs.txt',
  turn_plsql_constructs_audit_file: '_audit/{taskId}_turn{turnNumber}_7plSqlConstructs.audit.json',
};

export function getTaskOutputDir(env = process.env) {
  return env.TASK_OUTPUT_DIR ? pathResolve(env.TASK_OUTPUT_DIR) : DEFAULT_TASK_OUTPUT_DIR;
}

export function getSchemaCacheDir(env = process.env) {
  return env.SCHEMA_CACHE_DIR
    ? pathResolve(env.SCHEMA_CACHE_DIR)
    : pathResolve(getTaskOutputDir(env), DEFAULT_SCHEMA_CACHE_DIR_NAME);
}

export function formatTaskArtifactName(templateKey, values = {}) {
  const template = TASK_ARTIFACT_TEMPLATES[templateKey];

  if (!template) {
    throw new Error(`Unknown task artifact template: ${templateKey}`);
  }

  return template
    .replace(/\{taskId\}/g, values.taskId ?? '')
    .replace(/\{turnNumber\}/g, values.turnNumber ?? '');
}



