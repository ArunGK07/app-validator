import { CommonModule } from '@angular/common';
import { Component, DestroyRef, OnInit, inject } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { ActivatedRoute, RouterLink } from '@angular/router';
import { finalize, firstValueFrom } from 'rxjs';

import { DashboardApiService } from './dashboard-api.service';
import {
  ConversationRow,
  TaskFilters,
  TaskReport,
  TaskReportFile,
  TaskWorkflowAction,
  TaskWorkflowActionResult,
  ValidationMasterReport,
  ValidationRunSummary,
} from './models';

interface FileGroup {
  key: string;
  label: string;
  files: string[];
}

interface ValidationItem {
  label: string;
  value: string;
  tone?: 'neutral' | 'good' | 'warn';
}

interface ValidationSection {
  title: string;
  items: ValidationItem[];
}

interface ValidationFailureGroup {
  key: string;
  title: string;
  rows: Array<{
    item: string;
    present: string | null;
    expected: string | null;
    update: string | null;
    sourceFile: string | null;
  }>;
}

@Component({
  selector: 'app-report-page',
  standalone: true,
  imports: [CommonModule, RouterLink],
  templateUrl: './report-page.component.html',
  styleUrl: './report-page.component.css',
})
export class ReportPageComponent implements OnInit {
  private readonly api = inject(DashboardApiService);
  private readonly destroyRef = inject(DestroyRef);
  private readonly route = inject(ActivatedRoute);

  taskId = '';
  report: TaskReport | null = null;
  selectedFileName = '';
  fileGroups: FileGroup[] = [];
  activeGroupKey = 'all';
  fileContent: TaskReportFile | null = null;
  validationReport: ValidationMasterReport | null = null;
  loadingReport = false;
  loadingFile = false;
  error = '';
  actionError = '';
  actionMessage = '';
  loadingFetch = false;
  runningAction: TaskWorkflowAction | null = null;
  lastActionResult: TaskWorkflowActionResult | null = null;
  readonly workflowActions: Array<{ action: TaskWorkflowAction; label: string }> = [
    { action: 'validate', label: 'Re-Validate' },
    { action: 'generate-outputs', label: 'Generate Outputs' },
    { action: 'publish', label: 'Publish' },
  ];

  ngOnInit(): void {
    this.route.queryParamMap.pipe(takeUntilDestroyed(this.destroyRef)).subscribe((params) => {
      const taskId = params.get('taskId') ?? '';

      this.taskId = taskId;
      this.error = '';
      this.report = null;
      this.fileGroups = [];
      this.activeGroupKey = 'all';
      this.fileContent = null;
      this.validationReport = null;
      this.selectedFileName = '';
      this.actionError = '';
      this.actionMessage = '';
      this.loadingFetch = false;
      this.runningAction = null;
      this.lastActionResult = null;

      if (!taskId) {
        return;
      }

      this.loadReport(taskId);
    });
  }

  selectFile(name: string): void {
    if (!this.taskId || !name) {
      return;
    }

    this.selectedFileName = name;
    this.loadingFile = true;

    this.api
      .getTaskReportFile(this.taskId, name)
      .pipe(finalize(() => (this.loadingFile = false)))
      .subscribe({
        next: (file) => {
          this.fileContent = file;
        },
        error: (error: unknown) => {
          this.error = this.asErrorMessage(error);
          this.fileContent = null;
        },
      });
  }

  isSelectedFile(name: string): boolean {
    return this.selectedFileName === name;
  }

  async runWorkflowAction(action: TaskWorkflowAction): Promise<void> {
    if (!this.taskId || this.isTaskActionDisabled()) {
      return;
    }

    this.runningAction = action;
    this.actionError = '';
    this.actionMessage = `${this.getActionLabel(action)} started for task ${this.taskId}.`;

    this.api
      .runTaskWorkflowAction(this.taskId, action)
      .pipe(finalize(() => (this.runningAction = null)))
      .subscribe({
        next: (result) => {
          this.lastActionResult = result;
          void this.loadValidationReportFromAction(result);
          this.actionMessage = result.success
            ? `${this.getActionLabel(action)} completed for task ${this.taskId}.`
            : `${this.getActionLabel(action)} finished with exit code ${result.exitCode}.`;

          this.loadReport(this.taskId);
        },
        error: (error: unknown) => {
          this.actionError = this.asErrorMessage(error);
        },
      });
  }

  async fetchTaskData(): Promise<void> {
    if (!this.taskId || this.isTaskActionDisabled()) {
      return;
    }

    const taskId = this.taskId;
    this.loadingFetch = true;
    this.actionError = '';
    this.actionMessage = `Fetch started for task ${taskId}.`;

    try {
      const rows = await firstValueFrom(this.api.getConversations(this.buildTaskLookupFilters(taskId)));
      const row = rows.find((entry) => entry.taskId === taskId);
      const result = await firstValueFrom(this.api.fetchTaskOutput(taskId, this.buildTaskFetchPayload(row)));
      const generatedCount = result.generatedFiles.length;
      const graphqlNote = result.graphqlErrors ? ` GraphQL returned ${result.graphqlErrors} error(s).` : '';
      const metadataNote = result.metadataFile ? ` Metadata file ${result.metadataFile} is ready.` : '';
      const schemaNote = result.schemaFile
        ? ` Schema cache ${result.schemaFile} is ready.`
        : result.schemaError
          ? ` Schema generation failed: ${result.schemaError}`
          : '';

      if (this.taskId !== taskId) {
        return;
      }

      this.actionMessage = `Fetched task ${result.taskId} and generated ${generatedCount} file${generatedCount === 1 ? '' : 's'} in ${result.folderPath}.${graphqlNote}${metadataNote}${schemaNote}`;
      this.loadReport(taskId);
    } catch (error) {
      this.actionError = this.asErrorMessage(error);
    } finally {
      if (this.taskId === taskId) {
        this.loadingFetch = false;
      }
    }
  }

  isRunningAction(action: TaskWorkflowAction): boolean {
    return this.runningAction === action;
  }

  isTaskActionDisabled(): boolean {
    return this.loadingFetch || !!this.runningAction;
  }

  getActionLabel(action: TaskWorkflowAction): string {
    return this.workflowActions.find((entry) => entry.action === action)?.label ?? action;
  }

  getShortFileLabel(name: string): string {
    const normalizedName = name.replace(/\\/g, '/');
    const baseName = normalizedName.split('/').pop() ?? normalizedName;
    const withoutTaskPrefix = baseName.replace(new RegExp(`^${this.escapeForRegex(this.taskId)}_?`, 'i'), '');
    const withoutTurnPrefix = withoutTaskPrefix.replace(/^turn\d+_?/i, '');

    return withoutTurnPrefix || baseName;
  }

  isActiveGroup(key: string): boolean {
    return this.activeGroupKey === key;
  }

  showGroup(key: string): void {
    this.activeGroupKey = key;

    const files = this.visibleFiles;

    if (!files.length) {
      this.selectedFileName = '';
      this.fileContent = null;
      return;
    }

    if (!files.some((file) => file.name === this.selectedFileName)) {
      this.selectFile(files[0].name);
    }
  }

  get visibleFiles() {
    if (!this.report) {
      return [];
    }

    const group = this.fileGroups.find((entry) => entry.key === this.activeGroupKey);

    if (!group || group.key === 'all') {
      return this.report.files;
    }

    const visibleNames = new Set(group.files);
    return this.report.files.filter((file) => visibleNames.has(file.name));
  }

  formatFileSize(size: number): string {
    if (size < 1024) {
      return `${size} B`;
    }

    if (size < 1024 * 1024) {
      return `${(size / 1024).toFixed(1)} KB`;
    }

    return `${(size / (1024 * 1024)).toFixed(1)} MB`;
  }

  get validationSections(): ValidationSection[] {
    if (!this.fileContent) {
      return [];
    }

    const content = this.fileContent.content ?? '';
    const trimmed = content.trim();
    const lineCount = content ? content.split(/\r?\n/).length : 0;
    const wordCount = trimmed ? trimmed.split(/\s+/).length : 0;
    const turn = this.readTurnNumber(this.fileContent.name);
    const sections: ValidationSection[] = [
      {
        title: 'File Details',
        items: [
          { label: 'File Name', value: this.fileContent.name },
          { label: 'Type', value: this.fileContent.extension || 'Unknown' },
          { label: 'Size', value: this.formatFileSize(this.fileContent.size) },
          { label: 'Turn', value: turn === null ? 'General' : `Turn ${turn}` },
          { label: 'Modified', value: new Date(this.fileContent.modifiedAt).toLocaleString() },
        ],
      },
      {
        title: 'Content Checks',
        items: [
          { label: 'Has Content', value: trimmed ? 'Yes' : 'No', tone: trimmed ? 'good' : 'warn' },
          { label: 'Line Count', value: String(lineCount) },
          { label: 'Word Count', value: String(wordCount) },
          { label: 'Character Count', value: String(content.length) },
        ],
      },
    ];

    const typeSection = this.buildTypeSpecificSection(this.fileContent.extension, content);

    if (typeSection) {
      sections.push(typeSection);
    }

    return sections;
  }

  get lastActionSummary(): ValidationSection[] {
    if (!this.lastActionResult) {
      return [];
    }

    const validatorItems =
      this.lastActionResult.validators?.map((entry) => ({
        label: entry.validator,
        value: `${entry.summary.passed}/${entry.summary.total} pass`,
        tone: this.asTone(entry.success),
      })) ?? [];

    return [
      {
        title: 'Workflow Result',
        items: [
          { label: 'Action', value: this.getActionLabel(this.lastActionResult.action) },
          {
            label: 'Status',
            value: this.lastActionResult.success ? 'Success' : `Failed (${this.lastActionResult.exitCode})`,
            tone: this.lastActionResult.success ? 'good' : 'warn',
          },
          { label: 'Duration', value: `${(this.lastActionResult.durationMs / 1000).toFixed(1)} s` },
          { label: 'Log File', value: this.lastActionResult.logFile },
          ...validatorItems,
        ],
      },
      {
        title: 'Debug Details',
        items: [
          { label: 'Runtime', value: this.lastActionResult.command.join(' ') || 'native-validation' },
          { label: 'Working Directory', value: this.lastActionResult.workingDirectory || 'N/A' },
          { label: 'Artifacts', value: this.lastActionResult.artifacts.length ? this.lastActionResult.artifacts.join(', ') : 'None' },
          { label: 'Reports', value: this.lastActionResult.reports ? Object.values(this.lastActionResult.reports).join(', ') : 'None' },
        ],
      },
    ];
  }

  get validationSummarySections(): ValidationSection[] {
    if (!this.validationReport) {
      return [];
    }

    const validatorCards = this.validationReport.validators.map((entry: ValidationRunSummary) => ({
      title: entry.validator,
      items: [
        { label: 'Status', value: entry.success ? 'Pass' : 'Fail', tone: this.asTone(entry.success) },
        { label: 'Checks', value: `${entry.summary.passed}/${entry.summary.total}` },
        { label: 'Report', value: entry.reportFile },
      ],
    }));

    return [
      {
        title: 'Master Summary',
        items: [
          {
            label: 'Validators',
            value: `${this.validationReport.summary.validatorsPassed}/${this.validationReport.summary.validatorsRun}`,
            tone: this.asTone(this.validationReport.summary.validatorsFailed === 0),
          },
          {
            label: 'Checks',
            value: `${this.validationReport.summary.itemsPassed}/${this.validationReport.summary.itemsTotal}`,
            tone: this.asTone(this.validationReport.summary.itemsFailed === 0),
          },
          { label: 'Generated', value: new Date(this.validationReport.generatedAt).toLocaleString() },
        ],
      },
      ...validatorCards,
    ];
  }

  get validationFailureGroups(): ValidationFailureGroup[] {
    if (!this.validationReport) {
      return [];
    }

    const groups = new Map<string, ValidationFailureGroup>();
    for (const row of this.validationReport.results.filter((entry) => entry.status === 'FAIL')) {
      const key = `${row.validator}|${row.turnId ?? 'task'}`;
      const title = row.turnId === null ? `${row.validator} - Task` : `${row.validator} - Turn ${row.turnId}`;
      const existing = groups.get(key) ?? { key, title, rows: [] };
      existing.rows.push({
        item: row.item,
        present: row.present,
        expected: row.expected,
        update: row.update,
        sourceFile: row.sourceFile,
      });
      groups.set(key, existing);
    }

    return [...groups.values()];
  }

  private loadReport(taskId: string): void {
    this.loadingReport = true;

    this.api
      .getTaskReport(taskId)
      .pipe(finalize(() => (this.loadingReport = false)))
      .subscribe({
        next: (report) => {
          this.report = report;
          this.fileGroups = this.buildFileGroups(report);
          this.activeGroupKey = this.fileGroups[0]?.key ?? 'all';
          void this.loadPersistedValidationReport(report);

          if (report.files.length) {
            this.selectFile(this.visibleFiles[0]?.name ?? report.files[0].name);
          }
        },
        error: (error: unknown) => {
          this.error = this.asErrorMessage(error);
        },
      });
  }

  private buildTaskLookupFilters(taskId: string): TaskFilters {
    return {
      userId: '',
      taskIdQuery: taskId,
      status: 'all',
      batchId: '',
    };
  }

  private buildTaskFetchPayload(row?: ConversationRow): { promptId?: string; collabLink?: string; metadata?: unknown } {
    return {
      promptId: row?.promptId?.trim() || undefined,
      collabLink: row?.collabLink?.trim() || undefined,
      metadata: row?.metadata,
    };
  }

  private buildFileGroups(report: TaskReport): FileGroup[] {
    const allGroup: FileGroup = {
      key: 'all',
      label: `All files (${report.files.length})`,
      files: report.files.map((file) => file.name),
    };
    const generalFiles: string[] = [];
    const logFiles: string[] = [];
    const validationFiles: string[] = [];
    const turnGroups = new Map<number, string[]>();

    for (const file of report.files) {
      if (this.isLogFile(file.name)) {
        logFiles.push(file.name);
        continue;
      }

      if (this.isValidationFile(file.name)) {
        validationFiles.push(file.name);
        continue;
      }

      const turn = this.readTurnNumber(file.name);

      if (turn === null) {
        generalFiles.push(file.name);
        continue;
      }

      const existing = turnGroups.get(turn) ?? [];
      existing.push(file.name);
      turnGroups.set(turn, existing);
    }

    const groups: FileGroup[] = [allGroup];

    if (generalFiles.length) {
      groups.push({
        key: 'general',
        label: `General (${generalFiles.length})`,
        files: generalFiles,
      });
    }

    if (logFiles.length) {
      groups.push({
        key: 'logs',
        label: `Logs (${logFiles.length})`,
        files: logFiles,
      });
    }

    if (validationFiles.length) {
      groups.push({
        key: 'validation',
        label: `Validation (${validationFiles.length})`,
        files: validationFiles,
      });
    }

    for (const turn of [...turnGroups.keys()].sort((left, right) => left - right)) {
      const files = turnGroups.get(turn) ?? [];

      groups.push({
        key: `turn-${turn}`,
        label: `Turn ${turn} (${files.length})`,
        files,
      });
    }

    return groups;
  }

  private buildTypeSpecificSection(extension: string, content: string): ValidationSection | null {
    const normalized = extension.toLowerCase();

    if (normalized === 'json') {
      return this.buildJsonSection(content);
    }

    if (normalized === 'sql') {
      return this.buildSqlSection(content);
    }

    if (normalized === 'py') {
      return this.buildPythonSection(content);
    }

    if (normalized === 'txt') {
      return this.buildTextSection(content);
    }

    return null;
  }

  private buildJsonSection(content: string): ValidationSection {
    try {
      const parsed = JSON.parse(content);
      const topLevelType = Array.isArray(parsed) ? 'Array' : typeof parsed === 'object' && parsed !== null ? 'Object' : typeof parsed;
      const itemCount =
        Array.isArray(parsed) ? parsed.length : typeof parsed === 'object' && parsed !== null ? Object.keys(parsed).length : 0;

      return {
        title: 'JSON Validation',
        items: [
          { label: 'Valid JSON', value: 'Yes', tone: 'good' },
          { label: 'Top-Level Type', value: topLevelType },
          { label: 'Item Count', value: String(itemCount) },
        ],
      };
    } catch {
      return {
        title: 'JSON Validation',
        items: [{ label: 'Valid JSON', value: 'No', tone: 'warn' }],
      };
    }
  }

  private buildSqlSection(content: string): ValidationSection {
    const statements = content
      .split(';')
      .map((value) => value.trim())
      .filter(Boolean);

    return {
      title: 'SQL Validation',
      items: [
        { label: 'Statement Count', value: String(statements.length) },
        { label: 'Has SELECT', value: /\bselect\b/i.test(content) ? 'Yes' : 'No', tone: /\bselect\b/i.test(content) ? 'good' : 'neutral' },
        { label: 'Has INSERT/UPDATE/DELETE', value: /\b(insert|update|delete)\b/i.test(content) ? 'Yes' : 'No' },
      ],
    };
  }

  private buildPythonSection(content: string): ValidationSection {
    const functionMatches = content.match(/^\s*def\s+/gm) ?? [];
    const importMatches = content.match(/^\s*(from\s+\S+\s+import|import\s+\S+)/gm) ?? [];

    return {
      title: 'Python Validation',
      items: [
        { label: 'Function Count', value: String(functionMatches.length) },
        { label: 'Import Count', value: String(importMatches.length) },
        { label: 'Has Main Guard', value: /if\s+__name__\s*==\s*['"]__main__['"]/.test(content) ? 'Yes' : 'No' },
      ],
    };
  }

  private buildTextSection(content: string): ValidationSection {
    const paragraphs = content
      .split(/\r?\n\r?\n/)
      .map((value) => value.trim())
      .filter(Boolean);

    return {
      title: 'Text Validation',
      items: [
        { label: 'Paragraph Count', value: String(paragraphs.length) },
        { label: 'Contains Bullet/List Markers', value: /(^|\n)\s*(?:[-*]|\u2022)\s+/m.test(content) ? 'Yes' : 'No' },
      ],
    };
  }

  private readTurnNumber(fileName: string): number | null {
    const match = fileName.replace(/\\/g, '/').match(/(?:^|\/|_)turn(\d+)(?:_|\.|\/|$)/i);

    return match ? Number(match[1]) : null;
  }

  private isLogFile(fileName: string): boolean {
    return /^_logs[\\/]/i.test(fileName);
  }

  private isValidationFile(fileName: string): boolean {
    return /^_validation[\\/]/i.test(fileName);
  }

  private async loadPersistedValidationReport(report: TaskReport): Promise<void> {
    const masterReport = report.files.find((file) => /_validation[\\/]+master_validator_task_/i.test(file.name));
    if (!masterReport || !this.taskId) {
      this.validationReport = null;
      return;
    }

    try {
      const file = await firstValueFrom(this.api.getTaskReportFile(this.taskId, masterReport.name));
      this.validationReport = JSON.parse(file.content) as ValidationMasterReport;
    } catch {
      this.validationReport = null;
    }
  }

  private async loadValidationReportFromAction(result: TaskWorkflowActionResult): Promise<void> {
    if (!this.taskId || !result.reports?.master) {
      return;
    }

    try {
      const file = await firstValueFrom(this.api.getTaskReportFile(this.taskId, result.reports.master));
      this.validationReport = JSON.parse(file.content) as ValidationMasterReport;
    } catch {
      this.validationReport = null;
    }
  }

  private escapeForRegex(value: string): string {
    return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  }

  private asTone(success: boolean): 'good' | 'warn' {
    return success ? 'good' : 'warn';
  }

  private asErrorMessage(error: unknown): string {
    if (typeof error === 'object' && error !== null && 'error' in error) {
      const nested = (error as { error?: { message?: string } }).error?.message;

      if (nested) {
        return nested;
      }
    }

    if (error instanceof Error) {
      return error.message;
    }

    return 'Something went wrong while loading report output.';
  }
}




