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
  source?: 'conversation' | 'task-output';
  lastReviewScore?: number | null;
  lastReviewFeedback?: string | null;
  lastReviewerName?: string | null;
  lastReviewStatus?: string | null;
  lastReviewType?: string | null;
  lastReviewFollowup?: boolean | null;
  updatedAt?: string | null;
}

export interface ReviewDetail {
  taskId: string;
  reviewId: string | null;
  score: number | null;
  feedback: string | null;
  status: string | null;
  reviewType: string | null;
  followupRequired: boolean | null;
  reviewerName: string | null;
  reviewerEmail: string | null;
  audit: unknown;
  qualityDimensions: ReviewQualityDimension[];
}

export interface ReviewQualityDimension {
  name: string;
  score: number | null;
  weight: number | null;
  scoreText: string | null;
  feedback: string | null;
  trainerFeedback: string | null;
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

export type TaskWorkflowAction = 'validate' | 'generate-outputs' | 'generate-artifacts' | 'execute-tests' | 'publish';

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

export interface ValidationChecklistCatalogEntry {
  checkId: string;
  category: string;
  validator: string;
  item?: string;
  itemPrefix?: string;
  dynamic?: boolean;
  ruleIds: string[];
  description: string;
}

export interface ValidationChecklistEntry {
  checkId: string;
  category: string;
  validator: string;
  item: string;
  ruleId: string | null;
  description: string | null;
  status: 'PASS' | 'FAIL' | 'NOT_RUN';
  taskId: string | null;
  turnId: number | null;
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

export interface ValidationFileRunSummary {
  validator: string;
  success: boolean;
  summary: ValidationSummary;
}

export interface ValidationFileReportIndexEntry {
  sourceFile: string;
  turnId: number | null;
  reportFile: string;
  logFile: string;
  summary: ValidationSummary;
  validators: ValidationFileRunSummary[];
}

export interface ValidationReportFiles {
  master: string;
  promptStructure: string;
  plsqlCombined: string;
  namingStandard: string;
  artifactAlignment: string;
  fileIndex: string;
}

export interface ValidationFileReport {
  validator: string;
  generatedAt: string;
  taskId: string;
  sourceFile: string;
  turnId: number | null;
  summary: ValidationSummary;
  validators: ValidationFileRunSummary[];
  checklist: ValidationChecklistEntry[];
  results: ValidationResultRow[];
}

export interface ValidationMasterReport {
  validator: string;
  generatedAt: string;
  taskId: string;
  summary: ValidationMasterSummary;
  validators: ValidationRunSummary[];
  checklistCatalog: ValidationChecklistCatalogEntry[];
  checklist: ValidationChecklistEntry[];
  results: ValidationResultRow[];
  fileReports: ValidationFileReportIndexEntry[];
}
