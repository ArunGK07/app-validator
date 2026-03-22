import { readFile, writeFile } from 'node:fs/promises';
import { join } from 'node:path';

import { getSharedSchemaPath } from '../schema-cache.mjs';
import { generateTaskSchemaArtifact } from '../schema-extractor.mjs';
import { loadTaskMetadata, loadTurnTextArtifact } from '../validation/common.mjs';
import { formatTaskArtifactName } from '../workspace-config.mjs';
import { analyzeColumns, analyzeConstructs, analyzeReasoningTypes, analyzeTables, evaluatePlsqlConstructs, evaluateReasoningTypes, formatCommaLines } from './analyzers.mjs';
import { refreshTaskTestCases } from './testcase-runner.mjs';

export async function runNativeGenerateOutputs(taskId, taskDir, logFilePath, config, dependencies = {}) {
  const startedAt = new Date();
  const artifacts = [];
  const logLines = [`Generate outputs started for task ${taskId}`];
  const metadataResult = await loadTaskMetadata(taskDir, taskId);

  if (!metadataResult.metadata) {
    const message = metadataResult.errors[0]?.present || `metadata missing for task ${taskId}`;
    await writeFile(logFilePath, `${message}\n`, 'utf8');
    return buildResult(false, startedAt, taskId, taskDir, logFilePath, artifacts, { message });
  }

  const metadata = metadataResult.metadata;
  const numTurns = Number.parseInt(String(metadata?.num_turns ?? 0), 10);
  let schema = null;

  try {
    const schemaGenerator = dependencies.schemaGenerator ?? generateTaskSchemaArtifact;
    await schemaGenerator({ taskId, taskDir, metadata }, config, dependencies.schemaOptions ?? {});
    const schemaPath = getSharedSchemaPath(metadata, config.schemaCacheDir);
    if (schemaPath) {
      schema = JSON.parse(await readFile(schemaPath, 'utf8'));
      artifacts.push(schemaPath);
      logLines.push(`Loaded schema cache ${schemaPath}`);
    }
  } catch (error) {
    logLines.push(`Schema generation skipped: ${error instanceof Error ? error.message : 'unknown error'}`);
  }

  for (let turnNumber = 1; turnNumber <= numTurns; turnNumber += 1) {
    const codeArtifact = await loadTurnTextArtifact(taskDir, 'turn_reference_answer_file', taskId, turnNumber);
    if (!codeArtifact.text) {
      logLines.push(`Turn ${turnNumber}: missing ${codeArtifact.fileName}`);
      continue;
    }

    if (schema) {
      const tables = analyzeTables(codeArtifact.text, schema).map((table) => `${String(schema.database).toUpperCase()}.${table.toUpperCase()}`);
      const tablesPath = join(taskDir, formatTaskArtifactName('turn_tables_file', { taskId, turnNumber }));
      await writeFile(tablesPath, `${tables.length ? formatCommaLines(tables) : '[No tables found]'}\n`, 'utf8');
      artifacts.push(tablesPath);

      const columns = analyzeColumns(codeArtifact.text, schema);
      const columnsPath = join(taskDir, formatTaskArtifactName('turn_columns_file', { taskId, turnNumber }));
      await writeFile(columnsPath, `${columns.length ? formatCommaLines(columns) : '[No columns found]'}\n`, 'utf8');
      artifacts.push(columnsPath);
    }

    const constructEvaluations = evaluatePlsqlConstructs(codeArtifact.text);
    const constructs = analyzeConstructs(codeArtifact.text);
    const constructsPath = join(taskDir, formatTaskArtifactName('turn_plsql_constructs_file', { taskId, turnNumber }));
    await writeFile(constructsPath, `${constructs.length ? formatCommaLines(constructs) : '[NO PL/SQL CONSTRUCTS DETECTED]'}\n`, 'utf8');
    artifacts.push(constructsPath);
    const constructsAuditPath = join(taskDir, formatTaskArtifactName('turn_plsql_constructs_audit_file', { taskId, turnNumber }));
    await writeFile(
      constructsAuditPath,
      JSON.stringify(
        {
          taskId: String(taskId),
          turnNumber,
          sourceFile: codeArtifact.fileName,
          catalogSource: 'PLSQL_CONSTRUCTS_LIST.pdf',
          totalItemsConsidered: constructEvaluations.length,
          matchedItemsCount: constructEvaluations.filter((entry) => entry.matched).length,
          items: constructEvaluations,
        },
        null,
        2,
      ),
      'utf8',
    );
    artifacts.push(constructsAuditPath);

    const reasoningEvaluations = evaluateReasoningTypes(codeArtifact.text);
    const reasoningTypes = analyzeReasoningTypes(codeArtifact.text);
    const reasoningPath = join(taskDir, formatTaskArtifactName('turn_reasoning_types_file', { taskId, turnNumber }));
    await writeFile(reasoningPath, `${reasoningTypes.length ? formatCommaLines(reasoningTypes) : '[NO REASONING TYPES DETECTED]'}\n`, 'utf8');
    artifacts.push(reasoningPath);
    const reasoningAuditPath = join(taskDir, formatTaskArtifactName('turn_reasoning_types_audit_file', { taskId, turnNumber }));
    await writeFile(
      reasoningAuditPath,
      JSON.stringify(
        {
          taskId: String(taskId),
          turnNumber,
          sourceFile: codeArtifact.fileName,
          catalogSource: 'PLSQL_REASONING_TYPES_LIST.pdf',
          totalItemsConsidered: reasoningEvaluations.length,
          matchedItemsCount: reasoningEvaluations.filter((entry) => entry.matched).length,
          items: reasoningEvaluations,
        },
        null,
        2,
      ),
      'utf8',
    );
    artifacts.push(reasoningAuditPath);

    logLines.push(`Turn ${turnNumber}: analyzer artifacts written`);
    logLines.push(
      `Turn ${turnNumber}: PL/SQL constructs considered ${constructEvaluations.length}, matched ${constructEvaluations.filter((entry) => entry.matched).length}`,
    );
    logLines.push(
      `Turn ${turnNumber}: reasoning types considered ${reasoningEvaluations.length}, matched ${reasoningEvaluations.filter((entry) => entry.matched).length}`,
    );
  }

  const testcaseRunner = dependencies.testcaseRunner ?? refreshTaskTestCases;
  const testcaseResult = await testcaseRunner(taskId, taskDir, metadata, config, dependencies.testcaseOptions ?? {});
  artifacts.push(...(testcaseResult.updatedFiles ?? []));
  logLines.push(...(testcaseResult.logLines ?? []));

  await writeFile(logFilePath, `${logLines.join('\n')}\n`, 'utf8');
  return buildResult(true, startedAt, taskId, taskDir, logFilePath, [...new Set(artifacts)], {
    turnsProcessed: numTurns,
    artifactsWritten: [...new Set(artifacts)].length,
    testcaseFilesUpdated: testcaseResult.updatedFiles?.length ?? 0,
  });
}

function buildResult(success, startedAt, taskId, taskDir, logFilePath, artifacts, summary) {
  const finishedAt = new Date();
  return {
    action: 'generate-outputs',
    taskId: String(taskId),
    success,
    exitCode: success ? 0 : 1,
    startedAt: startedAt.toISOString(),
    finishedAt: finishedAt.toISOString(),
    durationMs: finishedAt.getTime() - startedAt.getTime(),
    scriptPath: '',
    workingDirectory: taskDir,
    command: ['native-generate-outputs'],
    logFile: logFilePath,
    stdoutTail: '',
    stderrTail: success ? '' : String(summary?.message ?? ''),
    artifacts,
    summary,
  };
}
