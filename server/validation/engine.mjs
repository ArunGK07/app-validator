import { writeFile } from 'node:fs/promises';
import { join } from 'node:path';

import {
  VALIDATOR_NAMES,
  buildReportNames,
  ensureValidationDir,
  formatLogText,
  loadTaskMetadata,
  summarizeResults,
  writeMasterValidationReport,
  writeValidationReport,
} from './common.mjs';
import { runPromptStructureValidator } from './prompt-structure-validator.mjs';
import { runPlsqlProgramValidator } from './plsql-program-validator.mjs';
import { runComplexityTableCountValidator } from './complexity-table-count-validator.mjs';
import { runNamingStandardValidator } from './naming-standard-validator.mjs';
import { runArtifactAlignmentValidator } from './artifact-alignment-validator.mjs';

export async function runNativeValidation(taskId, taskDir, logFilePath, dependencies = {}) {
  const startedAt = new Date();
  const validationDir = await ensureValidationDir(taskDir);
  const reportNames = buildReportNames(taskId);
  const metadataResult = await loadTaskMetadata(taskDir, taskId);

  const promptResults = metadataResult.metadata
    ? await runPromptStructureValidator(taskId, taskDir, metadataResult.metadata)
    : metadataResult.errors;
  const programResults = metadataResult.metadata
    ? await runPlsqlProgramValidator(taskId, taskDir, metadataResult.metadata)
    : [];
  const complexityResults = metadataResult.metadata
    ? await runComplexityTableCountValidator(taskId, taskDir, metadataResult.metadata)
    : [];
  const namingResults = metadataResult.metadata
    ? await runNamingStandardValidator(taskId, taskDir, metadataResult.metadata, dependencies.naming ?? {})
    : [];
  const artifactAlignmentResults = metadataResult.metadata
    ? await runArtifactAlignmentValidator(taskId, taskDir, metadataResult.metadata)
    : [];

  const validatorReports = [
    {
      validator: VALIDATOR_NAMES.promptStructure,
      reportFile: `_validation/${reportNames.promptStructure}`,
      results: promptResults,
      summary: summarizeResults(promptResults),
    },
    {
      validator: VALIDATOR_NAMES.plsqlProgram,
      reportFile: `_validation/${reportNames.plsqlCombined}`,
      results: [...complexityResults, ...programResults],
      summary: summarizeResults([...complexityResults, ...programResults]),
    },
    {
      validator: VALIDATOR_NAMES.namingStandard,
      reportFile: `_validation/${reportNames.namingStandard}`,
      results: namingResults,
      summary: summarizeResults(namingResults),
    },
    {
      validator: VALIDATOR_NAMES.artifactAlignment,
      reportFile: `_validation/${reportNames.artifactAlignment}`,
      results: artifactAlignmentResults,
      summary: summarizeResults(artifactAlignmentResults),
    },
  ];

  const promptReportPath = join(validationDir, reportNames.promptStructure);
  const combinedReportPath = join(validationDir, reportNames.plsqlCombined);
  const namingReportPath = join(validationDir, reportNames.namingStandard);
  const artifactAlignmentReportPath = join(validationDir, reportNames.artifactAlignment);
  const masterReportPath = join(validationDir, reportNames.master);

  const promptReport = await writeValidationReport(promptReportPath, VALIDATOR_NAMES.promptStructure, promptResults);
  const combinedReport = await writeValidationReport(
    combinedReportPath,
    VALIDATOR_NAMES.plsqlProgram,
    [...complexityResults, ...programResults],
    {
      validators: [
        { validator: VALIDATOR_NAMES.complexityTableCount, summary: summarizeResults(complexityResults) },
        { validator: VALIDATOR_NAMES.plsqlProgram, summary: summarizeResults(programResults) },
      ],
    },
  );
  const namingReport = await writeValidationReport(namingReportPath, VALIDATOR_NAMES.namingStandard, namingResults);
  const artifactAlignmentReport = await writeValidationReport(
    artifactAlignmentReportPath,
    VALIDATOR_NAMES.artifactAlignment,
    artifactAlignmentResults,
  );
  const masterReport = await writeMasterValidationReport(masterReportPath, taskId, validatorReports);

  await writeFile(logFilePath, formatLogText(masterReport, validatorReports), 'utf8');

  const finishedAt = new Date();
  return {
    action: 'validate',
    taskId: String(taskId),
    success: masterReport.summary.itemsFailed === 0 && masterReport.summary.validatorsFailed === 0,
    exitCode: masterReport.summary.itemsFailed === 0 && masterReport.summary.validatorsFailed === 0 ? 0 : 1,
    startedAt: startedAt.toISOString(),
    finishedAt: finishedAt.toISOString(),
    durationMs: finishedAt.getTime() - startedAt.getTime(),
    scriptPath: '',
    workingDirectory: taskDir,
    command: ['native-validation'],
    logFile: logFilePath,
    stdoutTail: '',
    stderrTail: '',
    artifacts: [
      promptReportPath,
      combinedReportPath,
      namingReportPath,
      artifactAlignmentReportPath,
      masterReportPath,
      join(validationDir, reportNames.fileIndex),
      ...masterReport.fileReports.flatMap((entry) => [
        join(taskDir, entry.reportFile),
        join(taskDir, entry.logFile),
      ]),
    ],
    summary: masterReport.summary,
    validators: validatorReports.map((report) => ({
      validator: report.validator,
      success: report.summary.failed === 0,
      summary: report.summary,
      reportFile: report.reportFile,
    })),
    reports: {
      master: `_validation/${reportNames.master}`,
      promptStructure: `_validation/${reportNames.promptStructure}`,
      plsqlCombined: `_validation/${reportNames.plsqlCombined}`,
      namingStandard: `_validation/${reportNames.namingStandard}`,
      artifactAlignment: `_validation/${reportNames.artifactAlignment}`,
      fileIndex: `_validation/${reportNames.fileIndex}`,
    },
    reportPayloads: {
      master: masterReport,
      promptStructure: promptReport,
      plsqlCombined: combinedReport,
      namingStandard: namingReport,
      artifactAlignment: artifactAlignmentReport,
    },
  };
}
