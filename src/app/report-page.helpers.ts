import {
  TaskOutputFile,
  TaskReport,
  ValidationChecklistEntry,
  ValidationFileReportIndexEntry,
  ValidationMasterReport,
} from './models';

export interface FileGroup {
  key: string;
  label: string;
  files: string[];
}

export type CanonicalArtifactType =
  | 'prompt'
  | 'tables'
  | 'columns'
  | 'plsql-program'
  | 'test-cases'
  | 'reasoning-types'
  | 'plsql-constructors';

export interface CombinedTurnReviewFile {
  name: string;
  artifactType: CanonicalArtifactType;
}

export interface CombinedTurnReviewSection {
  turnId: number;
  files: CombinedTurnReviewFile[];
}

export interface RecalculationIssue {
  validator?: string | null;
  item?: string | null;
  ruleId?: string | null;
  turnId?: number | null;
  sourceFile?: string | null;
}

const CANONICAL_ARTIFACT_ORDER: CanonicalArtifactType[] = [
  'prompt',
  'tables',
  'columns',
  'plsql-program',
  'test-cases',
  'reasoning-types',
  'plsql-constructors',
];

const CANONICAL_ARTIFACT_RANK = new Map<CanonicalArtifactType, number>(
  CANONICAL_ARTIFACT_ORDER.map((type, index) => [type, index]),
);

export function getShortFileLabel(taskId: string, name: string): string {
  const normalizedName = name.replace(/\\/g, '/');
  const baseName = normalizedName.split('/').pop() ?? normalizedName;
  const withoutTaskPrefix = taskId ? baseName.replace(new RegExp(`^${escapeForRegex(taskId)}_?`, 'i'), '') : baseName;
  const withoutTurnPrefix = withoutTaskPrefix.replace(/^turn\d+_?/i, '');
  return withoutTurnPrefix || baseName;
}

export function readTurnNumber(fileName: string): number | null {
  const match = fileName.replace(/\\/g, '/').match(/(?:^|\/|_)turn(\d+)(?:_|\.|\/|$)/i);
  return match ? Number(match[1]) : null;
}

export function classifyCanonicalArtifact(taskId: string, fileName: string): CanonicalArtifactType | null {
  const shortLabel = getShortFileLabel(taskId, fileName).trim().toLowerCase();

  switch (shortLabel) {
    case '1user.txt':
      return 'prompt';
    case '2tables.txt':
      return 'tables';
    case '3columns.txt':
      return 'columns';
    case '4referenceanswer.sql':
      return 'plsql-program';
    case '5testcases.sql':
      return 'test-cases';
    case '6reasoningtypes.txt':
      return 'reasoning-types';
    case '7plsqlconstructs.txt':
      return 'plsql-constructors';
    default:
      return null;
  }
}

export function sortByCanonicalArtifactOrder(taskId: string, fileNames: string[]): string[] {
  return [...fileNames].sort((left, right) => {
    const leftType = classifyCanonicalArtifact(taskId, left);
    const rightType = classifyCanonicalArtifact(taskId, right);
    const leftRank = leftType === null ? Number.MAX_SAFE_INTEGER : (CANONICAL_ARTIFACT_RANK.get(leftType) ?? Number.MAX_SAFE_INTEGER);
    const rightRank = rightType === null ? Number.MAX_SAFE_INTEGER : (CANONICAL_ARTIFACT_RANK.get(rightType) ?? Number.MAX_SAFE_INTEGER);

    if (leftRank !== rightRank) {
      return leftRank - rightRank;
    }

    return left.localeCompare(right, undefined, { numeric: true, sensitivity: 'base' });
  });
}

export function isLogFile(fileName: string): boolean {
  return /^_logs[\\/]/i.test(fileName);
}

export function isValidationFile(fileName: string): boolean {
  return /^_validation[\\/]/i.test(fileName);
}

export function isAuditFile(fileName: string): boolean {
  return /(?:^_audit[\\/]|\.audit\.json$)/i.test(fileName);
}

export function isInternalValidationFile(fileName: string): boolean {
  return /^_validation[\\/]files[\\/]/i.test(fileName);
}

export function buildFileGroups(report: TaskReport): FileGroup[] {
  const generalFiles: string[] = [];
  const logFiles: string[] = [];
  const auditFiles: string[] = [];
  const validationFiles: string[] = [];
  const turnGroups = new Map<number, string[]>();

  for (const file of report.files) {
    if (isInternalValidationFile(file.name)) {
      continue;
    }

    if (isLogFile(file.name)) {
      logFiles.push(file.name);
      continue;
    }

    if (isAuditFile(file.name)) {
      auditFiles.push(file.name);
      continue;
    }

    if (isValidationFile(file.name)) {
      validationFiles.push(file.name);
      continue;
    }

    const turn = readTurnNumber(file.name);
    if (turn === null) {
      generalFiles.push(file.name);
      continue;
    }

    const existing = turnGroups.get(turn) ?? [];
    existing.push(file.name);
    turnGroups.set(turn, existing);
  }

  const groups: FileGroup[] = [];

  for (const turn of [...turnGroups.keys()].sort((left, right) => left - right)) {
    const files = turnGroups.get(turn) ?? [];
    groups.push({
      key: `turn-${turn}`,
      label: `Turn ${turn} (${files.length})`,
      files,
    });
  }

  if (generalFiles.length) {
    groups.push({ key: 'general', label: `General (${generalFiles.length})`, files: generalFiles });
  }

  if (logFiles.length) {
    groups.push({ key: 'logs', label: `Logs (${logFiles.length})`, files: logFiles });
  }

  if (auditFiles.length) {
    groups.push({ key: 'audit', label: `Audit (${auditFiles.length})`, files: auditFiles });
  }

  if (validationFiles.length) {
    groups.push({ key: 'validation', label: `Validation (${validationFiles.length})`, files: validationFiles });
  }

  return groups;
}

export function buildCombinedTurnReviewSection(report: TaskReport, turnId: number): CombinedTurnReviewSection | null {
  const files = sortByCanonicalArtifactOrder(
    report.taskId,
    report.files
      .filter((file) => readTurnNumber(file.name) === turnId)
      .map((file) => file.name)
      .filter((fileName) => classifyCanonicalArtifact(report.taskId, fileName) !== null),
  ).map((name) => ({
    name,
    artifactType: classifyCanonicalArtifact(report.taskId, name)!,
  }));

  if (!files.length) {
    return null;
  }

  return {
    turnId,
    files,
  };
}

export function buildCombinedTaskReviewSections(report: TaskReport): CombinedTurnReviewSection[] {
  const turnIds = [...new Set(
    report.files
      .map((file) => file.name)
      .filter((fileName) => classifyCanonicalArtifact(report.taskId, fileName) !== null)
      .map((fileName) => readTurnNumber(fileName))
      .filter((turnId): turnId is number => turnId !== null),
  )].sort((left, right) => left - right);

  return turnIds
    .map((turnId) => buildCombinedTurnReviewSection(report, turnId))
    .filter((section): section is CombinedTurnReviewSection => section !== null);
}

export function resolveReportFileName(reportFiles: TaskOutputFile[], sourceFile: string): string | null {
  const normalizedSource = sourceFile.replace(/\\/g, '/').toLowerCase();
  const exactMatch = reportFiles.find((file) => file.name.replace(/\\/g, '/').toLowerCase() === normalizedSource);
  if (exactMatch) {
    return exactMatch.name;
  }

  const sourceBaseName = normalizedSource.split('/').pop();
  if (!sourceBaseName) {
    return null;
  }

  const baseMatch = reportFiles.find((file) => {
    const normalizedFile = file.name.replace(/\\/g, '/').toLowerCase();
    return normalizedFile.split('/').pop() === sourceBaseName;
  });

  return baseMatch?.name ?? null;
}

export function matchesCurrentFile(selectedFileName: string, reportFiles: TaskOutputFile[], sourceFile: string | null): boolean {
  if (!sourceFile || !selectedFileName) {
    return false;
  }

  return resolveReportFileName(reportFiles, sourceFile) === selectedFileName;
}

export function buildValidationStatusCache(validationReport: ValidationMasterReport | null, reportFiles: TaskOutputFile[]) {
  const fileStates = new Map<string, 'fail' | 'success'>();
  const turnStates = new Map<string, 'fail' | 'success'>();

  if (validationReport) {
    const applyState = (
      map: Map<string, 'fail' | 'success'>,
      key: string | null | undefined,
      state: 'fail' | 'success',
    ): void => {
      if (!key) {
        return;
      }

      const current = map.get(key);
      if (current === 'fail') {
        return;
      }

      if (state === 'fail' || !current) {
        map.set(key, state);
      }
    };

    for (const row of validationReport.results) {
      const state: 'fail' | 'success' = row.status === 'FAIL' ? 'fail' : 'success';
      const resolvedFile = row.sourceFile ? resolveReportFileName(reportFiles, row.sourceFile) : null;
      applyState(fileStates, resolvedFile, state);
      applyState(turnStates, row.turnId === null ? null : `turn-${row.turnId}`, state);
    }

    for (const row of validationReport.checklist) {
      if (row.status === 'NOT_RUN') {
        continue;
      }

      const state: 'fail' | 'success' = row.status === 'FAIL' ? 'fail' : 'success';
      const resolvedFile = row.sourceFile ? resolveReportFileName(reportFiles, row.sourceFile) : null;
      applyState(fileStates, resolvedFile, state);
      applyState(turnStates, row.turnId === null ? null : `turn-${row.turnId}`, state);
    }
  }

  return {
    report: validationReport,
    fileStates,
    turnStates,
  };
}

export function findFileValidationEntry(
  validationReport: ValidationMasterReport | null,
  reportFiles: TaskOutputFile[],
  selectedFileName: string,
): ValidationFileReportIndexEntry | null {
  if (!validationReport || !selectedFileName) {
    return null;
  }

  return validationReport.fileReports.find((entry) => resolveReportFileName(reportFiles, entry.sourceFile) === selectedFileName) ?? null;
}

export function filterChecklistRows(
  rows: ValidationChecklistEntry[],
  reportFiles: TaskOutputFile[],
  selectedFileName: string,
  showCurrentFileErrorsOnly: boolean,
  showFailuresOnly: boolean,
): ValidationChecklistEntry[] {
  return rows.filter((row) => {
    if (showCurrentFileErrorsOnly && !matchesCurrentFile(selectedFileName, reportFiles, row.sourceFile)) {
      return false;
    }

    if (showCurrentFileErrorsOnly || showFailuresOnly) {
      return row.status === 'FAIL';
    }

    return true;
  });
}

export function resolveRecalculationFilesForIssue(report: TaskReport | null, issue: RecalculationIssue): string[] {
  if (!report || !shouldIncludeAllTurnFiles(issue)) {
    return [];
  }

  const groups = buildFileGroups(report);

  if (Number.isInteger(issue.turnId)) {
    return groups.find((group) => group.key === `turn-${issue.turnId}`)?.files ?? [];
  }

  const resolvedFile = issue.sourceFile ? resolveReportFileName(report.files, issue.sourceFile) : null;
  if (resolvedFile) {
    return groups.find((group) => group.key.startsWith('turn-') && group.files.includes(resolvedFile))?.files ?? [];
  }

  return [];
}

function shouldIncludeAllTurnFiles(issue: RecalculationIssue): boolean {
  return issue.validator === 'ComplexityTableCountValidator'
    && issue.item === 'Complexity Table Count'
    && issue.ruleId === 'count_mismatch';
}

function escapeForRegex(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}


