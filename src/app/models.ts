export interface TeamMember {
  id: string;
  name: string;
  email: string;
}

export interface TaskFilters {
  userId: string;
  taskIdQuery: string;
  status: string;
  batchId: string;
}

export interface ConversationRow {
  taskId: string;
  metadata: unknown;
  metadataPreview: string;
  status: string;
  businessStatus: string;
  turnCount: string;
  complexity: string;
  batchId: string;
  batch: string;
  schemaName: string;
  assignedUser: string;
  promptId?: string;
  collabLink?: string;
}

export interface BatchOption {
  id: string;
  name: string;
}

export interface TaskOutputFile {
  name: string;
  size: number;
  modifiedAt: string;
  extension: string;
}

export interface TaskReport {
  taskId: string;
  folderPath: string;
  files: TaskOutputFile[];
}

export interface TaskReportFile {
  taskId: string;
  name: string;
  size: number;
  modifiedAt: string;
  extension: string;
  content: string;
}

export type TaskWorkflowAction = 'validate' | 'generate-outputs' | 'publish';

export interface TaskFetchResult {
  taskId: string;
  promptId: string;
  collabLink: string | null;
  graphqlUrl: string;
  folderPath: string;
  existingOutputFile: string;
  metadataFile: string | null;
  generatedFiles: string[];
  schemaFile: string | null;
  schemaError: string | null;
  graphqlErrors: number;
}

export interface TaskWorkflowActionResult {
  action: TaskWorkflowAction;
  taskId: string;
  success: boolean;
  exitCode: number;
  startedAt: string;
  finishedAt: string;
  durationMs: number;
  scriptPath: string;
  workingDirectory: string;
  command: string[];
  logFile: string;
  stdoutTail: string;
  stderrTail: string;
  artifacts: string[];
  summary?: ValidationSummary | ValidationMasterSummary;
  validators?: ValidationRunSummary[];
  reports?: ValidationReportFiles;
}

export interface HealthResponse {
  configured: boolean;
  message: string;
}

export interface ValidationSummary {
  total: number;
  passed: number;
  failed: number;
  tasksTotal: number;
  tasksPassed: number;
  tasksFailed: number;
}

export interface ValidationMasterSummary {
  validatorsRun: number;
  validatorsPassed: number;
  validatorsFailed: number;
  itemsTotal: number;
  itemsPassed: number;
  itemsFailed: number;
  taskFailHits: number;
  tasksTotal: number;
  tasksPassed: number;
  tasksFailed: number;
}

export interface ValidationResultRow {
  taskId: string;
  turnId: number | null;
  validator: string;
  item: string;
  ruleId: string;
  status: 'PASS' | 'FAIL';
  expected: string | null;
  present: string | null;
  update: string | null;
  sourceFile: string | null;
  line: number | null;
}

export interface ValidationRunSummary {
  validator: string;
  success: boolean;
  summary: ValidationSummary;
  reportFile: string;
}

export interface ValidationReportFiles {
  master: string;
  promptStructure: string;
  plsqlCombined: string;
  namingStandard: string;
}

export interface ValidationMasterReport {
  validator: string;
  generatedAt: string;
  taskId: string;
  summary: ValidationMasterSummary;
  validators: ValidationRunSummary[];
  results: ValidationResultRow[];
}
