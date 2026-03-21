import { CommonModule } from '@angular/common';
import { Component, DestroyRef, ElementRef, OnInit, ViewChild, inject } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { FormsModule } from '@angular/forms';
import { ActivatedRoute, RouterLink } from '@angular/router';
import { finalize, firstValueFrom } from 'rxjs';

import { DashboardApiService } from './dashboard-api.service';
import {
  ConversationRow,
  TaskFilters,
  ValidationChecklistEntry,
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
    line: number | null;
  }>;
}

interface ValidationChecklistGroup {
  key: string;
  title: string;
  rows: ValidationChecklistEntry[];
}

interface PreviewLogLine {
  lineNumber: number;
  text: string;
  sourceFile: string | null;
  sourceLine: number | null;
}

interface TopValidationAlert {
  validator: string;
  item: string;
  turnId: number | null;
  description: string | null;
  present: string | null;
  expected: string | null;
  sourceFile: string | null;
  line: number | null;
}

@Component({
  selector: 'app-report-page',
  standalone: true,
  imports: [CommonModule, FormsModule, RouterLink],
  templateUrl: './report-page.component.html',
  styleUrl: './report-page.component.css',
})
export class ReportPageComponent implements OnInit {
  private static readonly AUTO_SAVE_DELAY_MS = 500;
  private static readonly AUTO_SAVE_RETRY_DELAY_MS = 2000;
  private readonly api = inject(DashboardApiService);
  private readonly destroyRef = inject(DestroyRef);
  private readonly route = inject(ActivatedRoute);
  @ViewChild('fileEditor') private fileEditorRef?: ElementRef<HTMLTextAreaElement>;
  @ViewChild('readOnlyPreview') private readOnlyPreviewRef?: ElementRef<HTMLElement>;
  private autoSaveTimer: ReturnType<typeof setTimeout> | null = null;
  private saveRequest: Promise<boolean> | null = null;
  private validationStatusCache:
    | {
        report: ValidationMasterReport | null;
        fileStates: Map<string, 'fail' | 'success'>;
        turnStates: Map<string, 'fail' | 'success'>;
      }
    | null = null;

  taskId = '';
  report: TaskReport | null = null;
  selectedFileName = '';
  selectedFileByGroup: Record<string, string> = {};
  selectedFilePreference = '';
  fileGroups: FileGroup[] = [];
  activeGroupKey = 'all';
  fileContent: TaskReportFile | null = null;
  validationReport: ValidationMasterReport | null = null;
  showCurrentFileErrorsOnly = false;
  loadingReport = false;
  loadingFile = false;
  error = '';
  actionError = '';
  actionMessage = '';
  loadingFetch = false;
  runningAction: TaskWorkflowAction | null = null;
  lastActionResult: TaskWorkflowActionResult | null = null;
  editableContent = '';
  editingFile = false;
  savingFile = false;
  pendingAutoSave = false;
  lastSavedAt: string | null = null;
  lastSaveError = '';
  previewTargetLine: number | null = null;
  readonly workflowActions: Array<{ action: TaskWorkflowAction; label: string }> = [
    { action: 'validate', label: 'Re-Validate' },
    { action: 'generate-outputs', label: 'Generate Outputs' },
    { action: 'publish', label: 'Publish' },
  ];

  ngOnInit(): void {
    this.destroyRef.onDestroy(() => this.clearAutoSaveTimer());
    this.route.queryParamMap.pipe(takeUntilDestroyed(this.destroyRef)).subscribe((params) => {
      const taskId = params.get('taskId') ?? '';

      this.taskId = taskId;
      this.error = '';
      this.report = null;
      this.selectedFileByGroup = {};
      this.selectedFilePreference = '';
      this.fileGroups = [];
      this.activeGroupKey = 'all';
      this.fileContent = null;
      this.editableContent = '';
      this.editingFile = false;
      this.pendingAutoSave = false;
      this.lastSavedAt = null;
      this.lastSaveError = '';
      this.previewTargetLine = null;
      this.validationReport = null;
      this.showCurrentFileErrorsOnly = false;
      this.validationStatusCache = null;
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

  async selectFile(name: string, options?: { targetLine?: number | null }): Promise<void> {
    if (!this.taskId || !name) {
      return;
    }

    const didFlush = await this.flushPendingEdits();
    if (!didFlush) {
      return;
    }

    this.selectedFileName = name;
    this.selectedFileByGroup[this.activeGroupKey] = name;
    this.selectedFilePreference = this.getSelectionKey(name);
    this.loadingFile = true;
    this.previewTargetLine = options?.targetLine ?? null;
    this.clearAutoSaveTimer();
    this.pendingAutoSave = false;

    this.api
      .getTaskReportFile(this.taskId, name)
      .pipe(finalize(() => (this.loadingFile = false)))
      .subscribe({
        next: (file) => {
          this.fileContent = file;
          this.editableContent = file.content;
          this.editingFile = false;
          this.lastSavedAt = null;
          this.lastSaveError = '';
          this.schedulePreviewNavigation();
        },
        error: (error: unknown) => {
          this.error = this.asErrorMessage(error);
          this.fileContent = null;
          this.editableContent = '';
          this.editingFile = false;
          this.lastSavedAt = null;
          this.lastSaveError = '';
          this.previewTargetLine = null;
        },
      });
  }

  isSelectedFile(name: string): boolean {
    return this.selectedFileName === name;
  }

  getFileValidationState(fileName: string): 'fail' | 'success' | 'neutral' {
    this.ensureValidationStatusCache();
    return this.validationStatusCache?.fileStates.get(fileName) ?? 'neutral';
  }

  getGroupValidationState(groupKey: string): 'fail' | 'success' | 'neutral' {
    if (!this.validationReport) {
      return 'neutral';
    }

    this.ensureValidationStatusCache();

    if (groupKey.startsWith('turn-')) {
      return this.validationStatusCache?.turnStates.get(groupKey) ?? 'neutral';
    }

    const group = this.fileGroups.find((entry) => entry.key === groupKey);
    if (!group?.files.length) {
      return 'neutral';
    }

    const states = group.files
      .map((file) => this.getFileValidationState(file))
      .filter((state) => state !== 'neutral');

    if (!states.length) {
      return 'neutral';
    }

    return states.includes('fail') ? 'fail' : 'success';
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

  private getSelectionKey(name: string): string {
    return this.getShortFileLabel(name).trim().toLowerCase();
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

  async showGroup(key: string): Promise<void> {
    const didFlush = await this.flushPendingEdits();
    if (!didFlush) {
      return;
    }

    this.activeGroupKey = key;

    const files = this.visibleFiles;

    if (!files.length) {
      this.selectedFileName = '';
      this.fileContent = null;
      this.editableContent = '';
      this.editingFile = false;
      this.pendingAutoSave = false;
      this.lastSavedAt = null;
      this.lastSaveError = '';
      this.previewTargetLine = null;
      return;
    }

    const preferredFile = this.resolvePreferredFile(files);
    if (preferredFile && preferredFile !== this.selectedFileName) {
      void this.selectFile(preferredFile);
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

  get isEditableFile(): boolean {
    return this.fileContent ? ['txt', 'sql'].includes(this.fileContent.extension.toLowerCase()) : false;
  }

  get hasUnsavedChanges(): boolean {
    return Boolean(this.fileContent) && this.isEditableFile && this.editableContent !== this.fileContent?.content;
  }

  get showEditor(): boolean {
    return this.isEditableFile && this.editingFile;
  }

  get editorChangeMessage(): string {
    if (!this.showEditor) {
      return '';
    }

    if (this.savingFile) {
      return 'Saving changes...';
    }

    if (this.lastSaveError && this.hasUnsavedChanges) {
      return `Auto-save failed. Retrying... ${this.lastSaveError}`;
    }

    if (this.hasUnsavedChanges && this.pendingAutoSave) {
      return 'Unsaved changes detected. Saving shortly...';
    }

    if (this.hasUnsavedChanges) {
      return 'Unsaved changes detected.';
    }

    if (this.lastSavedAt) {
      return `All changes saved at ${new Date(this.lastSavedAt).toLocaleTimeString()}.`;
    }

    return 'No unsaved changes.';
  }

  get previewStatusMessage(): string {
    if (!this.fileContent || !this.previewTargetLine) {
      return '';
    }

    return `Focused on validation source at line ${this.previewTargetLine}.`;
  }

  get fileContentLines(): string[] {
    if (!this.fileContent) {
      return [];
    }

    return this.fileContent.content.split(/\r?\n/);
  }

  get previewLogLines(): PreviewLogLine[] {
    if (!this.fileContent) {
      return [];
    }

    return this.fileContentLines.map((text, index) => {
      const parsed = this.parseLogSourceLine(text);
      return {
        lineNumber: index + 1,
        text,
        sourceFile: parsed?.sourceFile ?? null,
        sourceLine: parsed?.line ?? null,
      };
    });
  }

  get showStructuredLogView(): boolean {
    return Boolean(this.fileContent && this.isLogFile(this.fileContent.name));
  }

  enableEditMode(): void {
    if (!this.isEditableFile || !this.fileContent) {
      return;
    }

    this.editingFile = true;
    this.editableContent = this.fileContent.content;
    this.pendingAutoSave = false;
    this.lastSavedAt = null;
    this.lastSaveError = '';
    this.previewTargetLine = null;
    setTimeout(() => this.focusPreviewTarget(), 0);
  }

  async cancelEditMode(): Promise<void> {
    if (!this.fileContent || !this.isEditableFile) {
      return;
    }

    const didFlush = await this.flushPendingEdits();
    if (!didFlush) {
      return;
    }

    this.clearAutoSaveTimer();
    this.editingFile = false;
    this.pendingAutoSave = false;
    this.lastSaveError = '';
    this.editableContent = this.fileContent.content;
  }

  handleEditableContentChange(content: string): void {
    this.editableContent = content;
    this.lastSaveError = '';

    if (!this.showEditor) {
      return;
    }

    if (!this.hasUnsavedChanges) {
      this.clearAutoSaveTimer();
      this.pendingAutoSave = false;
      return;
    }

    this.pendingAutoSave = true;
    this.clearAutoSaveTimer();
    this.autoSaveTimer = setTimeout(() => {
      void this.persistEditableFile('auto');
    }, ReportPageComponent.AUTO_SAVE_DELAY_MS);
  }

  saveEditableFile(): void {
    void this.persistEditableFile('manual');
  }

  resetEditableFile(): void {
    if (!this.fileContent || !this.showEditor) {
      return;
    }

    this.clearAutoSaveTimer();
    this.pendingAutoSave = false;
    this.lastSaveError = '';
    this.editableContent = this.fileContent.content;
  }

  handleEditorKeydown(event: KeyboardEvent): void {
    if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === 's') {
      event.preventDefault();
      if (this.hasUnsavedChanges && !this.savingFile) {
        this.saveEditableFile();
      }
      return;
    }

    if (event.key !== 'Tab') {
      return;
    }

    event.preventDefault();

    const target = event.target;
    if (!(target instanceof HTMLTextAreaElement)) {
      return;
    }

    const start = target.selectionStart ?? 0;
    const end = target.selectionEnd ?? start;
    const nextValue = `${this.editableContent.slice(0, start)}\t${this.editableContent.slice(end)}`;

    this.editableContent = nextValue;

    queueMicrotask(() => {
      target.selectionStart = start + 1;
      target.selectionEnd = start + 1;
    });
  }

  async handleEditorBlur(): Promise<void> {
    await this.persistEditableFile('auto');
  }

  async jumpToValidationSource(sourceFile: string | null, line: number | null): Promise<void> {
    if (!sourceFile) {
      return;
    }

    const didFlush = await this.flushPendingEdits();
    if (!didFlush) {
      return;
    }

    const resolvedFile = this.resolveReportFileName(sourceFile);
    if (!resolvedFile) {
      this.actionError = `Could not locate ${sourceFile} in the task output.`;
      return;
    }

    const owningGroup = this.fileGroups.find((group) => group.files.includes(resolvedFile));
    if (owningGroup) {
      this.activeGroupKey = owningGroup.key;
    }

    this.actionError = '';
    this.actionMessage = line ? `Opened ${resolvedFile} at line ${line}.` : `Opened ${resolvedFile}.`;
    await this.selectFile(resolvedFile, { targetLine: line });
  }

  formatSourceLocation(sourceFile: string | null, line: number | null): string {
    if (!sourceFile) {
      return 'No source location';
    }

    return line ? `${sourceFile}:${line}` : sourceFile;
  }

  logSourceLabel(line: PreviewLogLine): string {
    return this.formatSourceLocation(line.sourceFile, line.sourceLine);
  }

  isTargetPreviewLine(lineNumber: number): boolean {
    return this.previewTargetLine === lineNumber;
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

    const rows = this.showCurrentFileErrorsOnly
      ? this.validationReport.results.filter(
          (entry) => entry.status === 'FAIL' && this.matchesCurrentFile(entry.sourceFile),
        )
      : this.validationReport.results.filter((entry) => entry.status === 'FAIL');

    const groups = new Map<string, ValidationFailureGroup>();
    for (const row of rows) {
      const key = `${row.validator}|${row.turnId ?? 'task'}`;
      const title = row.turnId === null ? `${row.validator} - Task` : `${row.validator} - Turn ${row.turnId}`;
      const existing = groups.get(key) ?? { key, title, rows: [] };
      existing.rows.push({
        item: row.item,
        present: row.present,
        expected: row.expected,
        update: row.update,
        sourceFile: row.sourceFile,
        line: row.line,
      });
      groups.set(key, existing);
    }

    return [...groups.values()];
  }

  get validationChecklistGroups(): ValidationChecklistGroup[] {
    if (!this.validationReport?.checklist?.length) {
      return [];
    }

    const rows = this.showCurrentFileErrorsOnly
      ? this.validationReport.checklist.filter(
          (row) => row.status === 'FAIL' && this.matchesCurrentFile(row.sourceFile),
        )
      : this.validationReport.checklist;

    const groups = new Map<string, ValidationChecklistGroup>();
    for (const row of rows) {
      const key = `${row.category}|${row.validator}`;
      const title = `${this.toDisplayLabel(row.category)} - ${row.validator}`;
      const existing = groups.get(key) ?? { key, title, rows: [] };
      existing.rows.push(row);
      groups.set(key, existing);
    }

    return [...groups.values()].map((group) => ({
      ...group,
      rows: [...group.rows].sort((left, right) => left.item.localeCompare(right.item, undefined, { sensitivity: 'base' })),
    }));
  }

  get topValidationAlerts(): TopValidationAlert[] {
    if (!this.validationReport) {
      return [];
    }

    return this.validationReport.results
      .filter((entry) => entry.status === 'FAIL')
      .slice(0, 5)
      .map((entry) => ({
        validator: entry.validator,
        item: entry.item,
        turnId: entry.turnId,
        description: null,
        present: entry.present,
        expected: entry.expected,
        sourceFile: entry.sourceFile,
        line: entry.line,
      }));
  }

  get hasMoreValidationAlerts(): boolean {
    return Boolean(this.validationReport && this.validationReport.results.filter((entry) => entry.status === 'FAIL').length > this.topValidationAlerts.length);
  }

  get totalValidationFailures(): number {
    return this.validationReport?.summary.itemsFailed ?? 0;
  }

  get showSummarySections(): boolean {
    return !this.showCurrentFileErrorsOnly;
  }

  get hasValidationContent(): boolean {
    return (
      (this.showSummarySections &&
        (this.lastActionSummary.length > 0 || this.validationSummarySections.length > 0 || this.validationSections.length > 0)) ||
      this.validationChecklistGroups.length > 0 ||
      this.validationFailureGroups.length > 0
    );
  }

  formatValidationAlertTitle(alert: TopValidationAlert): string {
    const scope = alert.turnId === null ? 'Task' : `Turn ${alert.turnId}`;
    return `${alert.validator} - ${scope} - ${alert.item}`;
  }

  async copyValidationText(text: string): Promise<void> {
    const content = text.trim();
    if (!content) {
      return;
    }

    try {
      if (navigator.clipboard?.writeText) {
        await navigator.clipboard.writeText(content);
      } else {
        this.copyTextFallback(content);
      }
      this.actionError = '';
      this.actionMessage = 'Copied validation message.';
    } catch (error) {
      this.actionError = this.asErrorMessage(error);
    }
  }

  formatTopValidationAlertCopyText(alert: TopValidationAlert): string {
    return [
      this.formatValidationAlertTitle(alert),
      `Expected: ${alert.expected || 'This validation check should pass.'}`,
      `Found: ${alert.present || 'Validation failed.'}`,
      `Status: FAIL`,
      `File Name: ${alert.sourceFile ? this.formatSourceLocation(alert.sourceFile, alert.line) : 'N/A'}`,
    ].join('\n');
  }

  describeChecklistTest(row: ValidationChecklistEntry): string {
    return row.description || row.item;
  }

  describeChecklistExpectation(row: ValidationChecklistEntry): string {
    return row.expected || row.description || 'This validation check is expected to pass.';
  }

  describeChecklistFinding(row: ValidationChecklistEntry): string {
    if (row.status === 'PASS') {
      return row.present || 'Check passed.';
    }

    if (row.status === 'FAIL') {
      return row.present || 'Check failed.';
    }

    return 'Check not run.';
  }

  describeChecklistSource(row: ValidationChecklistEntry): string {
    if (!row.sourceFile) {
      return 'N/A';
    }

    return row.line ? `${row.sourceFile} (Line ${row.line})` : row.sourceFile;
  }

  formatChecklistCopyText(row: ValidationChecklistEntry): string {
    return [
      row.item,
      `Expected: ${this.describeChecklistExpectation(row)}`,
      `Found: ${this.describeChecklistFinding(row)}`,
      `Status: ${row.status}`,
      `File Name: ${this.describeChecklistSource(row)}`,
      row.update ? `Fix: ${row.update}` : null,
    ]
      .filter(Boolean)
      .join('\n');
  }

  formatFailureCopyText(row: ValidationFailureGroup['rows'][number], groupTitle: string): string {
    return [
      `${groupTitle} - ${row.item}`,
      row.expected ? `Expected: ${row.expected}` : null,
      `Found: ${row.present || row.expected || 'Failed'}`,
      'Status: FAIL',
      `File Name: ${row.sourceFile ? this.formatSourceLocation(row.sourceFile, row.line) : 'N/A'}`,
      row.update ? `Fix: ${row.update}` : null,
    ]
      .filter(Boolean)
      .join('\n');
  }

  private loadReport(taskId: string): void {
    this.loadingReport = true;

    this.api
      .getTaskReport(taskId)
      .pipe(finalize(() => (this.loadingReport = false)))
      .subscribe({
        next: (report) => {
          const currentGroupKey = this.activeGroupKey;
          this.report = report;
          this.fileGroups = this.buildFileGroups(report);
          this.validationStatusCache = null;
          this.activeGroupKey = this.fileGroups.some((group) => group.key === currentGroupKey)
            ? currentGroupKey
            : this.fileGroups[0]?.key ?? 'all';
          void this.loadPersistedValidationReport(report);

          if (report.files.length) {
            const preferredFile = this.resolvePreferredFile(this.visibleFiles);
            if (preferredFile) {
              void this.selectFile(preferredFile);
            }
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

    return groups;
  }

  private resolvePreferredFile(files: TaskReport['files']): string | null {
    if (!files.length) {
      return null;
    }

    const rememberedForGroup = this.selectedFileByGroup[this.activeGroupKey];
    if (rememberedForGroup && files.some((file) => file.name === rememberedForGroup)) {
      return rememberedForGroup;
    }

    if (this.selectedFileName && files.some((file) => file.name === this.selectedFileName)) {
      return this.selectedFileName;
    }

    if (this.selectedFilePreference) {
      const matchingFile = files.find((file) => this.getSelectionKey(file.name) === this.selectedFilePreference);
      if (matchingFile) {
        return matchingFile.name;
      }
    }

    return files[0]?.name ?? null;
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
      this.validationStatusCache = null;
      return;
    }

    try {
      const file = await firstValueFrom(this.api.getTaskReportFile(this.taskId, masterReport.name));
      this.validationReport = JSON.parse(file.content) as ValidationMasterReport;
      this.validationStatusCache = null;
    } catch {
      this.validationReport = null;
      this.validationStatusCache = null;
    }
  }

  private async loadValidationReportFromAction(result: TaskWorkflowActionResult): Promise<void> {
    if (!this.taskId || !result.reports?.master) {
      return;
    }

    try {
      const file = await firstValueFrom(this.api.getTaskReportFile(this.taskId, result.reports.master));
      this.validationReport = JSON.parse(file.content) as ValidationMasterReport;
      this.validationStatusCache = null;
    } catch {
      this.validationReport = null;
      this.validationStatusCache = null;
    }
  }

  private ensureValidationStatusCache(): void {
    if (this.validationStatusCache?.report === this.validationReport) {
      return;
    }

    const fileStates = new Map<string, 'fail' | 'success'>();
    const turnStates = new Map<string, 'fail' | 'success'>();
    const report = this.validationReport;

    if (report) {
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

      for (const row of report.results) {
        const state: 'fail' | 'success' = row.status === 'FAIL' ? 'fail' : 'success';
        const resolvedFile = row.sourceFile ? this.resolveReportFileName(row.sourceFile) : null;
        applyState(fileStates, resolvedFile, state);
        applyState(turnStates, row.turnId === null ? null : `turn-${row.turnId}`, state);
      }

      for (const row of report.checklist) {
        if (row.status === 'NOT_RUN') {
          continue;
        }

        const state: 'fail' | 'success' = row.status === 'FAIL' ? 'fail' : 'success';
        const resolvedFile = row.sourceFile ? this.resolveReportFileName(row.sourceFile) : null;
        applyState(fileStates, resolvedFile, state);
        applyState(turnStates, row.turnId === null ? null : `turn-${row.turnId}`, state);
      }
    }

    this.validationStatusCache = {
      report,
      fileStates,
      turnStates,
    };
  }

  private schedulePreviewNavigation(): void {
    if (!this.previewTargetLine || !this.fileContent) {
      return;
    }

    setTimeout(() => this.focusPreviewTarget(), 0);
  }

  private async persistEditableFile(mode: 'auto' | 'manual', options: { silent?: boolean } = {}): Promise<boolean> {
    if (!this.taskId || !this.fileContent || !this.showEditor || !this.hasUnsavedChanges) {
      return true;
    }

    if (this.saveRequest) {
      return this.saveRequest;
    }

    this.clearAutoSaveTimer();
    this.savingFile = true;
    this.pendingAutoSave = false;
    this.actionError = '';
    if (mode === 'manual' && !options.silent) {
      this.actionMessage = `Saving ${this.fileContent.name}...`;
    }

    this.saveRequest = (async () => {
      try {
        const file = await firstValueFrom(this.api.saveTaskReportFile(this.taskId, this.fileContent!.name, this.editableContent));
        this.fileContent = file;
        this.editableContent = file.content;
        this.lastSavedAt = file.modifiedAt;
        this.lastSaveError = '';
        this.syncReportFileMetadata(file);
        if (mode === 'manual' && !options.silent) {
          this.actionMessage = `Saved ${file.name}.`;
        }
        return true;
      } catch (error) {
        this.actionError = this.asErrorMessage(error);
        this.lastSaveError = this.actionError;
        if (this.hasUnsavedChanges) {
          this.pendingAutoSave = true;
          this.queueAutoSaveRetry();
        }
        return false;
      } finally {
        this.savingFile = false;
        this.saveRequest = null;
      }
    })();

    return this.saveRequest;
  }

  private async flushPendingEdits(): Promise<boolean> {
    if (!this.showEditor || !this.hasUnsavedChanges) {
      return true;
    }

    return this.persistEditableFile('manual', { silent: true });
  }

  private focusPreviewTarget(): void {
    if (!this.previewTargetLine || !this.fileContent) {
      return;
    }

    if (this.showEditor) {
      const editor = this.fileEditorRef?.nativeElement;
      if (!editor) {
        return;
      }

      const offset = this.findLineOffset(this.editableContent, this.previewTargetLine);
      const lineHeight = Number.parseFloat(getComputedStyle(editor).lineHeight) || 22;
      editor.focus();
      editor.selectionStart = offset;
      editor.selectionEnd = offset;
      editor.scrollTop = Math.max(0, (this.previewTargetLine - 1) * lineHeight - editor.clientHeight / 3);
      return;
    }

    const preview = this.readOnlyPreviewRef?.nativeElement;
    if (!preview) {
      return;
    }

    const target = preview.querySelector<HTMLElement>(`[data-line="${this.previewTargetLine}"]`);
    if (!target) {
      return;
    }

    preview.scrollTop = Math.max(0, target.offsetTop - preview.clientHeight / 3);
  }

  private findLineOffset(content: string, lineNumber: number): number {
    if (lineNumber <= 1) {
      return 0;
    }

    let currentLine = 1;
    for (let index = 0; index < content.length; index += 1) {
      if (content[index] !== '\n') {
        continue;
      }

      currentLine += 1;
      if (currentLine === lineNumber) {
        return index + 1;
      }
    }

    return content.length;
  }

  private resolveReportFileName(sourceFile: string): string | null {
    if (!this.report) {
      return null;
    }

    const normalizedSource = sourceFile.replace(/\\/g, '/').toLowerCase();
    const exactMatch = this.report.files.find((file) => file.name.replace(/\\/g, '/').toLowerCase() === normalizedSource);
    if (exactMatch) {
      return exactMatch.name;
    }

    const sourceBaseName = normalizedSource.split('/').pop();
    if (!sourceBaseName) {
      return null;
    }

    const baseMatch = this.report.files.find((file) => {
      const normalizedFile = file.name.replace(/\\/g, '/').toLowerCase();
      return normalizedFile.split('/').pop() === sourceBaseName;
    });

    return baseMatch?.name ?? null;
  }

  private matchesCurrentFile(sourceFile: string | null): boolean {
    if (!sourceFile || !this.selectedFileName) {
      return false;
    }

    return this.resolveReportFileName(sourceFile) === this.selectedFileName;
  }

  private syncReportFileMetadata(file: TaskReportFile): void {
    if (!this.report) {
      return;
    }

    this.report = {
      ...this.report,
      files: this.report.files.map((entry) =>
        entry.name === file.name
          ? {
              ...entry,
              size: file.size,
              modifiedAt: file.modifiedAt,
              extension: file.extension,
            }
          : entry,
      ),
    };
  }

  private parseLogSourceLine(text: string): { sourceFile: string; line: number | null } | null {
    const match = text.match(/^\s*source\s*:\s+(.+?)(?:\s+line\s+(\d+))?\s*$/i);
    if (!match) {
      return null;
    }

    const sourceFile = match[1]?.trim();
    if (!sourceFile) {
      return null;
    }

    return {
      sourceFile,
      line: match[2] ? Number.parseInt(match[2], 10) : null,
    };
  }

  private escapeForRegex(value: string): string {
    return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  }

  private clearAutoSaveTimer(): void {
    if (!this.autoSaveTimer) {
      return;
    }

    clearTimeout(this.autoSaveTimer);
    this.autoSaveTimer = null;
  }

  private queueAutoSaveRetry(): void {
    if (!this.showEditor || !this.hasUnsavedChanges) {
      return;
    }

    this.clearAutoSaveTimer();
    this.autoSaveTimer = setTimeout(() => {
      void this.persistEditableFile('auto');
    }, ReportPageComponent.AUTO_SAVE_RETRY_DELAY_MS);
  }

  private copyTextFallback(text: string): void {
    const textarea = document.createElement('textarea');
    textarea.value = text;
    textarea.setAttribute('readonly', 'true');
    textarea.style.position = 'fixed';
    textarea.style.opacity = '0';
    document.body.appendChild(textarea);
    textarea.select();
    document.execCommand('copy');
    document.body.removeChild(textarea);
  }

  private asTone(success: boolean): 'good' | 'warn' {
    return success ? 'good' : 'warn';
  }

  checklistTone(status: ValidationChecklistEntry['status']): 'good' | 'warn' | 'neutral' {
    if (status === 'PASS') {
      return 'good';
    }
    if (status === 'FAIL') {
      return 'warn';
    }
    return 'neutral';
  }

  private asErrorMessage(error: unknown): string {
    if (typeof error === 'object' && error !== null && 'error' in error) {
      const nested = (error as { error?: { message?: string } | string }).error;

      if (typeof nested === 'string' && nested.trim()) {
        return nested;
      }

      const nestedMessage = typeof nested === 'object' && nested !== null && 'message' in nested ? nested.message : null;

      if (typeof nestedMessage === 'string' && nestedMessage.trim()) {
        return nestedMessage;
      }
    }

    if (typeof error === 'object' && error !== null && 'message' in error && typeof error.message === 'string' && error.message.trim()) {
      return error.message;
    }

    if (error instanceof Error) {
      return error.message;
    }

    return 'Something went wrong while loading report output.';
  }

  private toDisplayLabel(value: string): string {
    return value
      .split(/[-_\s]+/)
      .filter(Boolean)
      .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
      .join(' ');
  }
}




