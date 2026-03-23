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
  ValidationFileReport,
  ValidationFileReportIndexEntry,
  TaskReport,
  TaskReportFile,
  TaskWorkflowAction,
  TaskWorkflowActionResult,
  ValidationMasterReport,
  ValidationRunSummary,
} from './models';
import {
  buildCombinedTaskReviewSections,
  buildCombinedTurnReviewSection,
  buildFileGroups as buildReportFileGroups,
  buildValidationStatusCache,
  CombinedTurnReviewSection,
  filterChecklistRows,
  findFileValidationEntry,
  getShortFileLabel as getReportFileShortLabel,
  isLogFile as isReportLogFile,
  isValidationFile as isReportValidationFile,
  matchesCurrentFile as matchesSelectedFile,
  readTurnNumber as readReportTurnNumber,
  resolveRecalculationFilesForIssue,
  resolveReportFileName as resolveNamedReportFile,
} from './report-page.helpers';

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

interface HeaderMetaItem {
  label: string;
  value: string;
}

interface HeaderRequirementItem {
  label: string;
  value: string;
  highlight: boolean;
}

interface ValidationFailureGroup {
  key: string;
  title: string;
  rows: Array<{
    validator: string;
    item: string;
    ruleId: string;
    turnId: number | null;
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
  ruleId: string;
  turnId: number | null;
  description: string | null;
  present: string | null;
  expected: string | null;
  sourceFile: string | null;
  line: number | null;
}

type ReviewMode = 'single' | 'turn' | 'task';

interface CombinedReviewFileViewModel {
  name: string;
  label: string;
  modifiedAt: string;
  content: string;
  lineCount: number;
}

interface CombinedReviewSectionViewModel {
  turnId: number;
  title: string;
  files: CombinedReviewFileViewModel[];
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
  private readonly labelingBaseUrl = 'https://labeling-o.turing.com';
  private readonly rlhfBaseUrl = 'https://rlhf-v3.turing.com';
  @ViewChild('fileEditor') private fileEditorRef?: ElementRef<HTMLTextAreaElement>;
  @ViewChild('readOnlyPreview') private readOnlyPreviewRef?: ElementRef<HTMLElement>;
  @ViewChild('reportGrid') private reportGridRef?: ElementRef<HTMLElement>;
  private autoSaveTimer: ReturnType<typeof setTimeout> | null = null;
  private saveRequest: Promise<boolean> | null = null;
  private panelResizeCleanup: (() => void) | null = null;
  private readonly defaultValidationPanelWidth = 360;
  private readonly minValidationPanelWidth = 280;
  private readonly minFilePreviewPanelWidth = 420;
  private validationStatusCache:
    | {
        report: ValidationMasterReport | null;
        fileStates: Map<string, 'fail' | 'success'>;
        turnStates: Map<string, 'fail' | 'success'>;
      }
    | null = null;

  taskId = '';
  headerSummaryRow: ConversationRow | null = null;
  report: TaskReport | null = null;
  headerMetaItems: HeaderMetaItem[] = [];
  headerRequirementItems: HeaderRequirementItem[] = [];
  headerRequirementSummary = '';
  selectedFileName = '';
  selectedFileByGroup: Record<string, string> = {};
  selectedFilePreference = '';
  fileGroups: FileGroup[] = [];
  activeGroupKey = 'all';
  reviewMode: ReviewMode = 'single';
  combinedLoading = false;
  combinedError = '';
  fileContentCache = new Map<string, TaskReportFile>();
  fileContent: TaskReportFile | null = null;
  activeFileView: 'content' | 'validation' | 'raw-log' = 'content';
  selectedFileValidationEntry: ValidationFileReportIndexEntry | null = null;
  selectedFileValidationReport: ValidationFileReport | null = null;
  selectedFileRawLog: TaskReportFile | null = null;
  validationReport: ValidationMasterReport | null = null;
  showCurrentFileErrorsOnly = false;
  showFailuresOnly = false;
  loadingReport = false;
  loadingFile = false;
  loadingFileValidation = false;
  loadingFileRawLog = false;
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
  validationPanelWidth = this.defaultValidationPanelWidth;
  readonly workflowActions: Array<{ action: TaskWorkflowAction; label: string }> = [
    { action: 'validate', label: 'Re-Validate' },
    { action: 'generate-outputs', label: 'Generate Outputs' },
    { action: 'publish', label: 'Publish' },
  ];

  ngOnInit(): void {
    this.destroyRef.onDestroy(() => {
      this.clearAutoSaveTimer();
      this.stopPanelResize();
    });
    this.route.queryParamMap.pipe(takeUntilDestroyed(this.destroyRef)).subscribe((params) => {
      const taskId = params.get('taskId') ?? '';

      this.taskId = taskId;
      this.error = '';
      this.headerSummaryRow = null;
      this.report = null;
      this.headerMetaItems = [];
      this.headerRequirementItems = [];
      this.headerRequirementSummary = '';
      this.selectedFileByGroup = {};
      this.selectedFilePreference = '';
      this.fileGroups = [];
      this.activeGroupKey = 'all';
      this.reviewMode = 'single';
      this.combinedLoading = false;
      this.combinedError = '';
      this.fileContentCache = new Map<string, TaskReportFile>();
      this.fileContent = null;
      this.activeFileView = 'content';
      this.selectedFileValidationEntry = null;
      this.selectedFileValidationReport = null;
      this.selectedFileRawLog = null;
      this.editableContent = '';
      this.editingFile = false;
      this.pendingAutoSave = false;
      this.lastSavedAt = null;
      this.lastSaveError = '';
      this.previewTargetLine = null;
      this.validationReport = null;
      this.showCurrentFileErrorsOnly = false;
      this.showFailuresOnly = false;
      this.validationStatusCache = null;
      this.loadingFileValidation = false;
      this.loadingFileRawLog = false;
      this.selectedFileName = '';
      this.actionError = '';
      this.actionMessage = '';
      this.loadingFetch = false;
      this.runningAction = null;
      this.lastActionResult = null;

      if (!taskId) {
        return;
      }

      this.loadTaskSummary(taskId);
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
    this.previewTargetLine = options?.targetLine ?? null;
    this.clearAutoSaveTimer();
    this.pendingAutoSave = false;
    this.resetSelectedFileArtifacts();

    const cachedFile = this.fileContentCache.get(name);
    if (cachedFile) {
      this.loadingFile = false;
      this.applySelectedFileContent(cachedFile);
      return;
    }

    this.loadingFile = true;

    try {
      const file = await firstValueFrom(this.api.getTaskReportFile(this.taskId, name));
      this.rememberFileContent(file);
      this.applySelectedFileContent(file);
    } catch (error) {
      this.error = this.asErrorMessage(error);
      this.fileContent = null;
      this.resetSelectedFileArtifacts();
      this.editableContent = '';
      this.editingFile = false;
      this.lastSavedAt = null;
      this.lastSaveError = '';
      this.previewTargetLine = null;
    } finally {
      this.loadingFile = false;
    }
  }

  isSelectedFile(name: string): boolean {
    return this.selectedFileName === name;
  }

  getFileValidationState(fileName: string): 'fail' | 'success' | 'neutral' {
    this.ensureValidationStatusCache();
    return this.validationStatusCache?.fileStates.get(fileName) ?? 'neutral';
  }

  getFileValidationBadge(fileName: string): string {
    const state = this.getFileValidationState(fileName);
    if (state === 'fail') {
      return 'Fail';
    }
    if (state === 'success') {
      return 'Pass';
    }
    return 'Open';
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

          this.loadReport(this.taskId, { preferredFileName: result.logFile });
        },
        error: (error: unknown) => {
          this.actionError = this.asErrorMessage(error);
          this.loadReport(this.taskId);
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

  getDisplayFileButtonLabel(name: string): string {
    const shortLabel = this.getShortFileLabel(name);

    switch (shortLabel.toLowerCase()) {
      case '1user.txt':
        return 'Prompt';
      case '2tables.txt':
        return 'Table';
      case '3columns.txt':
        return 'Columns';
      case '4referenceanswer.sql':
        return 'PL/SQL Program';
      case '5testcases.sql':
        return 'Test Cases';
      case '6reasoningtypes.txt':
        return 'Reasoning Types';
      case '7plsqlconstructs.txt':
        return 'PL/SQL Constructors';
      default:
        return shortLabel;
    }
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

  get isCombinedReviewMode(): boolean {
    return this.reviewMode !== 'single';
  }

  get showTurnGroupSelector(): boolean {
    return this.reviewMode !== 'task';
  }

  get showSingleFileStrip(): boolean {
    return this.reviewMode === 'single';
  }

  get combinedReviewSections(): CombinedReviewSectionViewModel[] {
    if (!this.report || !this.isCombinedReviewMode) {
      return [];
    }

    const sections = this.reviewMode === 'turn'
      ? this.buildTurnReviewSections()
      : this.buildTaskReviewSections();

    return sections
      .map((section) => this.toCombinedReviewSection(section))
      .filter((section): section is CombinedReviewSectionViewModel => section !== null);
  }

  get isTurnReviewUnavailable(): boolean {
    return this.reviewMode === 'turn' && !this.activeGroupKey.startsWith('turn-');
  }

  async selectReviewMode(mode: ReviewMode): Promise<void> {
    if (this.reviewMode === mode) {
      return;
    }

    const didFlush = await this.flushPendingEdits();
    if (!didFlush) {
      return;
    }

    this.reviewMode = mode;
    this.combinedError = '';
    this.activeFileView = 'content';

    if (mode !== 'single') {
      this.showCurrentFileErrorsOnly = false;
      void this.ensureCombinedReviewContentLoaded();
      return;
    }

    if (!this.report) {
      return;
    }

    const preferredFile = this.resolvePreferredFile(this.visibleFiles);
    if (preferredFile && preferredFile !== this.selectedFileName) {
      await this.selectFile(preferredFile);
    }
  }

  async openFileInSingleView(name: string): Promise<void> {
    const didFlush = await this.flushPendingEdits();
    if (!didFlush) {
      return;
    }

    const owningGroup = this.fileGroups.find((group) => group.files.includes(name));
    if (owningGroup) {
      this.activeGroupKey = owningGroup.key;
    }

    this.reviewMode = 'single';
    this.combinedError = '';
    this.activeFileView = 'content';
    await this.selectFile(name);
  }

  async showGroup(key: string): Promise<void> {
    const didFlush = await this.flushPendingEdits();
    if (!didFlush) {
      return;
    }

    this.activeGroupKey = key;

    if (this.reviewMode === 'turn') {
      this.combinedError = '';
      void this.ensureCombinedReviewContentLoaded();
      return;
    }

    if (this.reviewMode !== 'single') {
      return;
    }

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

  getContentLines(content: string): string[] {
    return content.split(/\r?\n/);
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

  get selectedFileValidationGroups(): ValidationChecklistGroup[] {
    if (!this.selectedFileValidationReport?.checklist?.length) {
      return [];
    }

    const rows = this.showFailuresOnly
      ? this.selectedFileValidationReport.checklist.filter((row) => row.status === 'FAIL')
      : this.selectedFileValidationReport.checklist;
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

  get selectedFileFailureGroups(): ValidationFailureGroup[] {
    if (!this.selectedFileValidationReport?.results?.length) {
      return [];
    }

    const rows = this.selectedFileValidationReport.results.filter((entry) => entry.status === 'FAIL');
    const groups = new Map<string, ValidationFailureGroup>();
    for (const row of rows) {
      const key = `${row.validator}|${row.turnId ?? 'task'}`;
      const title = row.turnId === null ? `${row.validator} - Task` : `${row.validator} - Turn ${row.turnId}`;
      const existing = groups.get(key) ?? { key, title, rows: [] };
      existing.rows.push({
        validator: row.validator,
        item: row.item,
        ruleId: row.ruleId,
        turnId: row.turnId,
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

  get selectedFileRawLogLines(): string[] {
    return this.selectedFileRawLog ? this.selectedFileRawLog.content.split(/\r?\n/) : [];
  }

  get selectedFileRawLogPreviewLines(): PreviewLogLine[] {
    return this.selectedFileRawLogLines.map((text, index) => {
      const parsed = this.parseLogSourceLine(text);
      return {
        lineNumber: index + 1,
        text,
        sourceFile: parsed?.sourceFile ?? null,
        sourceLine: parsed?.line ?? null,
      };
    });
  }

  get fileValidationSummaryItems(): ValidationItem[] {
    if (!this.selectedFileValidationEntry) {
      return [];
    }

    return [
      {
        label: 'Checks',
        value: `${this.selectedFileValidationEntry.summary.passed}/${this.selectedFileValidationEntry.summary.total}`,
        tone: this.asTone(this.selectedFileValidationEntry.summary.failed === 0),
      },
      { label: 'Report', value: this.selectedFileValidationEntry.reportFile },
      { label: 'Raw Log', value: this.selectedFileValidationEntry.logFile },
    ];
  }

  get hasSelectedFileValidationContent(): boolean {
    return this.fileValidationSummaryItems.length > 0 || this.selectedFileValidationGroups.length > 0 || this.selectedFileFailureGroups.length > 0;
  }

  selectFileView(view: 'content' | 'validation' | 'raw-log'): void {
    this.activeFileView = view;
    void this.ensureSelectedFileArtifactsLoaded();
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

    this.reviewMode = 'single';
    this.combinedError = '';
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
        validator: row.validator,
        item: row.item,
        ruleId: row.ruleId,
        turnId: row.turnId,
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

    const rows = filterChecklistRows(
      this.validationReport.checklist,
      this.report?.files ?? [],
      this.selectedFileName,
      this.showCurrentFileErrorsOnly,
      this.showFailuresOnly,
    );

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
        ruleId: entry.ruleId,
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

  get validationFilterDescription(): string {
    if (!this.validationReport) {
      return 'Validation results are task-wide. Source links open the related file when available.';
    }

    if (this.isCombinedReviewMode) {
      return 'Combined review keeps validation in task-wide mode. Source links open the related file.';
    }

    if (this.showCurrentFileErrorsOnly) {
      return 'Showing only failures linked to the selected file.';
    }

    if (this.showFailuresOnly) {
      return 'Showing only failures for task ' + this.validationReport.taskId + '. Source links open the related file.';
    }

    return 'Task-wide checklist and failures for task ' + this.validationReport.taskId + '. Source links open the related file.';
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
      ...this.getRecalculationFileLines(alert),
    ].join('\n');
  }

  getRecalculationFilesText(issue: {
    validator?: string | null;
    item?: string | null;
    ruleId?: string | null;
    turnId?: number | null;
    sourceFile?: string | null;
  }): string {
    return resolveRecalculationFilesForIssue(this.report, issue).join(', ');
  }

  private getRecalculationFileLines(issue: {
    validator?: string | null;
    item?: string | null;
    ruleId?: string | null;
    turnId?: number | null;
    sourceFile?: string | null;
  }): string[] {
    const files = resolveRecalculationFilesForIssue(this.report, issue);
    return files.length ? ['Files To Recalculate:', ...files.map((file) => `- ${file}`)] : [];
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
      ...this.getRecalculationFileLines(row),
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
      ...this.getRecalculationFileLines(row),
    ]
      .filter(Boolean)
      .join('\n');
  }

  private loadReport(taskId: string, options: { preferredFileName?: string | null } = {}): void {
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
          this.fileContentCache = new Map<string, TaskReportFile>();
          const preferredFile = options.preferredFileName ? this.resolveReportFileName(options.preferredFileName) : null;
          const preferredGroupKey = preferredFile
            ? this.fileGroups.find((group) => group.files.includes(preferredFile))?.key ?? null
            : null;

          this.activeGroupKey =
            preferredGroupKey
            ?? (this.fileGroups.some((group) => group.key === currentGroupKey) ? currentGroupKey : this.fileGroups[0]?.key ?? 'all');
          void this.loadPersistedValidationReport(report);

          if (!report.files.length) {
            this.fileContent = null;
            this.combinedError = '';
            return;
          }

          if (this.reviewMode === 'single') {
            const selectedFile = preferredFile ?? this.resolvePreferredFile(this.visibleFiles);
            if (selectedFile) {
              void this.selectFile(selectedFile);
            }
            return;
          }

          this.combinedError = '';
          void this.ensureCombinedReviewContentLoaded();
        },
        error: (error: unknown) => {
          this.error = this.asErrorMessage(error);
        },
      });
  }

  private loadTaskSummary(taskId: string): void {
    this.api
      .getConversations(this.buildTaskLookupFilters(taskId))
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe({
        next: (rows) => {
          if (this.taskId !== taskId) {
            return;
          }

          const row = rows.find((entry) => entry.taskId === taskId) ?? rows[0] ?? null;
          this.headerSummaryRow = row;
          this.headerMetaItems = this.buildHeaderMetaItems(row);
          this.headerRequirementItems = this.buildHeaderRequirementItems(row);
          this.headerRequirementSummary = this.buildHeaderRequirementSummary(row);
        },
        error: () => {
          if (this.taskId !== taskId) {
            return;
          }

          this.headerSummaryRow = null;
          this.headerMetaItems = [];
          this.headerRequirementItems = [];
          this.headerRequirementSummary = '';
        },
      });
  }

  private buildHeaderMetaItems(row: ConversationRow | null): HeaderMetaItem[] {
    if (!row) {
      return [];
    }

    const dataset = this.formatRequirementSummaryValue(this.findMetadataValue(row.metadata, 'dataset'));
    const database = this.formatRequirementSummaryValue(
      this.findMetadataValue(row.metadata, 'database') ?? row.schemaName,
    );

    return [
      { label: 'Dataset', value: dataset },
      { label: 'Database', value: database },
      { label: 'Turns', value: row.turnCount },
      { label: 'Complexity', value: row.complexity },
      { label: 'Status', value: row.businessStatus },
      { label: 'Owner', value: row.assignedUser },
      { label: 'Batch', value: row.batch },
    ].filter((item) => Boolean(item.value?.trim()));
  }

  private buildHeaderRequirementSummary(row: ConversationRow | null): string {
    if (!row) {
      return '';
    }

    if (this.buildHeaderRequirementItems(row).length) {
      return '';
    }

    const fallback = this.normalizeInlineText(row.metadataPreview);
    return fallback && !/^no metadata$/i.test(fallback) ? this.truncateInlineText(fallback, 220) : '';
  }

  private buildHeaderRequirementItems(row: ConversationRow | null): HeaderRequirementItem[] {
    if (!row) {
      return [];
    }

    const items = [
      { label: 'Cursors', value: this.findMetadataValue(row.metadata, 'required_cursors') },
      { label: 'Triggers', value: this.findMetadataValue(row.metadata, 'required_triggers') },
      { label: 'Dynamic SQL', value: this.findMetadataValue(row.metadata, 'required_dynamic_sql') },
      { label: 'Reasoning Types', value: this.findMetadataValue(row.metadata, 'target_reasoning_types') },
      { label: 'Debugging Task', value: this.findMetadataValue(row.metadata, 'required_debugging_task') },
      { label: 'Anonymous Block', value: this.findMetadataValue(row.metadata, 'required_anonymous_block') },
      { label: 'Procs/Funcs/Pkgs', value: this.findMetadataValue(row.metadata, 'required_procs_funcs_pkgs') },
      { label: 'Transaction Logic', value: this.findMetadataValue(row.metadata, 'required_transaction_logic') },
      { label: 'Exception Handling', value: this.findMetadataValue(row.metadata, 'required_exception_handling') },
      { label: 'Object Types/Modularization', value: this.findMetadataValue(row.metadata, 'required_object_types_or_modularization') },
    ];

    return items
      .map(({ label, value }) => {
        const formatted = this.formatRequirementSummaryValue(value);
        const normalized = formatted.toLowerCase();
        return formatted === '' || normalized === 'false'
          ? null
          : {
              label,
              value: formatted,
              highlight: normalized === 'true',
            };
      })
      .filter((entry): entry is HeaderRequirementItem => Boolean(entry))
      .sort((left, right) => {
        if (left.label === 'Reasoning Types') {
          return -1;
        }
        if (right.label === 'Reasoning Types') {
          return 1;
        }

        const leftRank = left.highlight ? 0 : 1;
        const rightRank = right.highlight ? 0 : 1;
        if (leftRank !== rightRank) {
          return leftRank - rightRank;
        }

        return 0;
      });
  }

  private findMetadataValue(value: unknown, targetKey: string, depth = 0): unknown {
    if (depth > 5 || value === null || value === undefined) {
      return undefined;
    }

    const normalizedTarget = targetKey.replace(/[^a-z0-9]/gi, '').toLowerCase();

    if (Array.isArray(value)) {
      for (const entry of value) {
        const found = this.findMetadataValue(entry, targetKey, depth + 1);
        if (found !== undefined) {
          return found;
        }
      }
      return undefined;
    }

    if (typeof value !== 'object') {
      return undefined;
    }

    for (const [entryKey, entryValue] of Object.entries(value as Record<string, unknown>)) {
      const normalizedEntryKey = entryKey.replace(/[^a-z0-9]/gi, '').toLowerCase();
      if (normalizedEntryKey === normalizedTarget) {
        return entryValue;
      }
    }

    for (const entryValue of Object.values(value as Record<string, unknown>)) {
      const found = this.findMetadataValue(entryValue, targetKey, depth + 1);
      if (found !== undefined) {
        return found;
      }
    }

    return undefined;
  }

  private formatRequirementSummaryValue(value: unknown): string {
    if (value === null || value === undefined) {
      return '';
    }

    if (typeof value === 'boolean') {
      return value ? 'true' : 'false';
    }

    if (typeof value === 'number') {
      return String(value);
    }

    if (Array.isArray(value)) {
      return value
        .map((entry) => this.normalizeInlineText(String(entry)))
        .filter(Boolean)
        .join(', ');
    }

    if (typeof value === 'string') {
      const trimmed = value.trim();
      if (!trimmed) {
        return '';
      }

      if (trimmed.startsWith('[') && trimmed.endsWith(']')) {
        try {
          const parsed = JSON.parse(trimmed);
          if (Array.isArray(parsed)) {
            return parsed.map((entry) => this.normalizeInlineText(String(entry))).filter(Boolean).join(', ');
          }
        } catch {
          return this.normalizeInlineText(trimmed);
        }
      }

      return this.normalizeInlineText(trimmed);
    }

    return this.normalizeInlineText(JSON.stringify(value));
  }

  private collectMetadataHighlights(
    value: unknown,
    path: string[] = [],
    results: string[] = [],
    depth = 0,
  ): string[] {
    if (results.length >= 4 || depth > 3 || value === null || value === undefined) {
      return results;
    }

    const key = path[path.length - 1] ?? '';

    if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
      const inlineValue = this.normalizeInlineText(String(value));
      if (!inlineValue || !key || this.isHeaderMetadataKeyExcluded(key)) {
        return results;
      }

      results.push(`${this.toHeaderMetadataLabel(key)}: ${this.truncateInlineText(inlineValue, 56)}`);
      return results;
    }

    if (Array.isArray(value)) {
      const primitiveValues = value
        .filter((entry) => typeof entry === 'string' || typeof entry === 'number' || typeof entry === 'boolean')
        .map((entry) => this.normalizeInlineText(String(entry)))
        .filter(Boolean)
        .slice(0, 3);

      if (primitiveValues.length && key && !this.isHeaderMetadataKeyExcluded(key)) {
        results.push(`${this.toHeaderMetadataLabel(key)}: ${this.truncateInlineText(primitiveValues.join(', '), 56)}`);
        return results;
      }

      for (const entry of value.slice(0, 3)) {
        this.collectMetadataHighlights(entry, path, results, depth + 1);
        if (results.length >= 4) {
          break;
        }
      }

      return results;
    }

    if (typeof value !== 'object') {
      return results;
    }

    for (const [entryKey, entryValue] of Object.entries(value as Record<string, unknown>)) {
      this.collectMetadataHighlights(entryValue, [...path, entryKey], results, depth + 1);
      if (results.length >= 4) {
        break;
      }
    }

    return results;
  }

  private isHeaderMetadataKeyExcluded(key: string): boolean {
    const normalizedKey = key.replace(/[^a-z0-9]/gi, '').toLowerCase();
    return [
      'id',
      'uuid',
      'taskid',
      'promptid',
      'promptuuid',
      'schema',
      'schemaname',
      'complexity',
      'difficulty',
      'turncount',
      'turns',
      'status',
      'assigneduser',
      'owner',
      'batch',
      'batchid',
      'collablink',
    ].includes(normalizedKey);
  }

  private toHeaderMetadataLabel(key: string): string {
    return key
      .replace(/([a-z0-9])([A-Z])/g, '$1 $2')
      .replace(/[_-]+/g, ' ')
      .replace(/\s+/g, ' ')
      .trim()
      .replace(/^./, (value) => value.toUpperCase());
  }

  private normalizeInlineText(value: string): string {
    return value.replace(/\s+/g, ' ').trim();
  }

  private truncateInlineText(value: string, maxLength: number): string {
    return value.length > maxLength ? `${value.slice(0, maxLength - 1).trimEnd()}...` : value;
  }

  getReportTaskHref(): string {
    return this.taskId ? `${this.labelingBaseUrl}/conversations/${this.taskId}/view` : this.labelingBaseUrl;
  }

  getReportCollabHref(): string | null {
    if (this.headerSummaryRow?.collabLink?.trim()) {
      return this.headerSummaryRow.collabLink.trim();
    }

    if (!this.headerSummaryRow?.promptId?.trim()) {
      return null;
    }

    const url = new URL(`prompt/${this.headerSummaryRow.promptId.trim()}`, `${this.rlhfBaseUrl}/`);
    url.searchParams.set('origin', this.labelingBaseUrl);
    url.searchParams.set('redirect_url', this.getReportTaskHref());
    return url.toString();
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
    return buildReportFileGroups(report);
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
    return readReportTurnNumber(fileName);
  }

  private isLogFile(fileName: string): boolean {
    return isReportLogFile(fileName);
  }

  private isValidationFile(fileName: string): boolean {
    return isReportValidationFile(fileName);
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
      this.refreshSelectedFileArtifacts();
    } catch {
      this.validationReport = null;
      this.validationStatusCache = null;
      this.resetSelectedFileArtifacts();
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
      this.refreshSelectedFileArtifacts();
    } catch {
      this.validationReport = null;
      this.validationStatusCache = null;
      this.resetSelectedFileArtifacts();
    }
  }

  private resetSelectedFileArtifacts(): void {
    this.selectedFileValidationEntry = null;
    this.selectedFileValidationReport = null;
    this.selectedFileRawLog = null;
    this.loadingFileValidation = false;
    this.loadingFileRawLog = false;
  }

  private refreshSelectedFileArtifacts(): void {
    if (!this.report || !this.selectedFileName) {
      this.resetSelectedFileArtifacts();
      return;
    }

    this.selectedFileValidationEntry = findFileValidationEntry(this.validationReport, this.report.files, this.selectedFileName);
    this.selectedFileValidationReport = null;
    this.selectedFileRawLog = null;
    this.loadingFileValidation = false;
    this.loadingFileRawLog = false;
    void this.ensureSelectedFileArtifactsLoaded();
  }

  private async ensureSelectedFileArtifactsLoaded(): Promise<void> {
    if (!this.taskId || !this.selectedFileValidationEntry) {
      return;
    }

    const loadTasks: Promise<void>[] = [];

    if (!this.selectedFileValidationReport && !this.loadingFileValidation) {
      this.loadingFileValidation = true;
      loadTasks.push((async () => {
        try {
          const file = await firstValueFrom(this.api.getTaskReportFile(this.taskId, this.selectedFileValidationEntry!.reportFile));
          this.selectedFileValidationReport = JSON.parse(file.content) as ValidationFileReport;
        } finally {
          this.loadingFileValidation = false;
        }
      })());
    }

    if (!this.selectedFileRawLog && !this.loadingFileRawLog) {
      this.loadingFileRawLog = true;
      loadTasks.push((async () => {
        try {
          this.selectedFileRawLog = await firstValueFrom(this.api.getTaskReportFile(this.taskId, this.selectedFileValidationEntry!.logFile));
        } finally {
          this.loadingFileRawLog = false;
        }
      })());
    }

    if (loadTasks.length) {
      await Promise.allSettled(loadTasks);
    }
  }

  private ensureValidationStatusCache(): void {
    if (this.validationStatusCache?.report === this.validationReport) {
      return;
    }

    this.validationStatusCache = buildValidationStatusCache(this.validationReport, this.report?.files ?? []);
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
        this.rememberFileContent(file);
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

  startPanelResize(event: PointerEvent): void {
    if (!this.isReportGridResizable()) {
      return;
    }

    const grid = this.reportGridRef?.nativeElement;
    if (!grid) {
      return;
    }

    event.preventDefault();
    this.stopPanelResize();

    const startX = event.clientX;
    const startWidth = this.validationPanelWidth;
    const bounds = this.getValidationPanelWidthBounds();
    const onMove = (moveEvent: PointerEvent) => {
      const delta = startX - moveEvent.clientX;
      this.validationPanelWidth = Math.round(this.clampValue(startWidth + delta, bounds.min, bounds.max));
    };
    const stop = () => {
      window.removeEventListener('pointermove', onMove);
      window.removeEventListener('pointerup', stop);
      window.removeEventListener('pointercancel', stop);
      document.body.style.removeProperty('cursor');
      document.body.style.removeProperty('user-select');
      this.panelResizeCleanup = null;
    };

    document.body.style.setProperty('cursor', 'col-resize');
    document.body.style.setProperty('user-select', 'none');
    window.addEventListener('pointermove', onMove);
    window.addEventListener('pointerup', stop);
    window.addEventListener('pointercancel', stop);
    this.panelResizeCleanup = stop;
  }

  handlePanelResizeKeydown(event: KeyboardEvent): void {
    if (!this.isReportGridResizable()) {
      return;
    }

    switch (event.key) {
      case 'ArrowLeft':
        event.preventDefault();
        this.adjustValidationPanelWidth(-24);
        break;
      case 'ArrowRight':
        event.preventDefault();
        this.adjustValidationPanelWidth(24);
        break;
      case 'Home':
        event.preventDefault();
        this.validationPanelWidth = this.getValidationPanelWidthBounds().min;
        break;
      case 'End':
        event.preventDefault();
        this.validationPanelWidth = this.getValidationPanelWidthBounds().max;
        break;
      case 'Enter':
      case ' ':
        event.preventDefault();
        this.resetValidationPanelWidth();
        break;
      default:
        break;
    }
  }

  resetValidationPanelWidth(): void {
    this.validationPanelWidth = this.getDefaultValidationPanelWidth();
  }

  private adjustValidationPanelWidth(delta: number): void {
    const bounds = this.getValidationPanelWidthBounds();
    this.validationPanelWidth = Math.round(this.clampValue(this.validationPanelWidth + delta, bounds.min, bounds.max));
  }

  private getDefaultValidationPanelWidth(): number {
    const bounds = this.getValidationPanelWidthBounds();
    return Math.round(this.clampValue(this.defaultValidationPanelWidth, bounds.min, bounds.max));
  }

  private getValidationPanelWidthBounds(): { min: number; max: number } {
    const gridWidth = this.reportGridRef?.nativeElement?.getBoundingClientRect().width ?? 0;
    const max =
      gridWidth > 0
        ? Math.max(this.minValidationPanelWidth, gridWidth - this.minFilePreviewPanelWidth - 12)
        : Math.max(this.minValidationPanelWidth, this.defaultValidationPanelWidth);

    return {
      min: this.minValidationPanelWidth,
      max,
    };
  }

  private isReportGridResizable(): boolean {
    return typeof window !== 'undefined' && window.matchMedia('(min-width: 1201px)').matches;
  }

  private stopPanelResize(): void {
    this.panelResizeCleanup?.();
  }

  private clampValue(value: number, min: number, max: number): number {
    return Math.min(Math.max(value, min), max);
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

  private buildTurnReviewSections(): CombinedTurnReviewSection[] {
    if (!this.report) {
      return [];
    }

    const turnId = this.getActiveTurnId();
    if (turnId === null) {
      return [];
    }

    const section = buildCombinedTurnReviewSection(this.report, turnId);
    return section ? [section] : [];
  }

  private buildTaskReviewSections(): CombinedTurnReviewSection[] {
    return this.report ? buildCombinedTaskReviewSections(this.report) : [];
  }

  private toCombinedReviewSection(section: CombinedTurnReviewSection): CombinedReviewSectionViewModel | null {
    if (!this.report) {
      return null;
    }

    const files = section.files
      .map((entry) => {
        const file = this.fileContentCache.get(entry.name);
        const metadata = this.report?.files.find((reportFile) => reportFile.name === entry.name);
        if (!file || !metadata) {
          return null;
        }

        return {
          name: entry.name,
          label: this.getDisplayFileButtonLabel(entry.name),
          modifiedAt: metadata.modifiedAt,
          content: file.content,
          lineCount: this.countLines(file.content),
        };
      })
      .filter((file): file is CombinedReviewFileViewModel => file !== null);

    if (!files.length) {
      return null;
    }

    return {
      turnId: section.turnId,
      title: `Turn ${section.turnId}`,
      files,
    };
  }

  private getActiveTurnId(): number | null {
    if (!this.activeGroupKey.startsWith('turn-')) {
      return null;
    }

    const parsed = Number.parseInt(this.activeGroupKey.slice(5), 10);
    return Number.isInteger(parsed) ? parsed : null;
  }

  private getCombinedReviewTargetFileNames(): string[] {
    const sections = this.reviewMode === 'turn'
      ? this.buildTurnReviewSections()
      : this.buildTaskReviewSections();

    return sections.flatMap((section) => section.files.map((file) => file.name));
  }

  private async ensureCombinedReviewContentLoaded(): Promise<void> {
    if (!this.taskId || !this.report || !this.isCombinedReviewMode) {
      this.combinedLoading = false;
      return;
    }

    const targetNames = this.getCombinedReviewTargetFileNames();
    if (!targetNames.length) {
      this.combinedLoading = false;
      this.combinedError = '';
      return;
    }

    const missingNames = targetNames.filter((name) => !this.fileContentCache.has(name));
    if (!missingNames.length) {
      this.combinedLoading = false;
      return;
    }

    const requestedTaskId = this.taskId;
    this.combinedLoading = true;
    this.combinedError = '';

    try {
      const files = await Promise.all(missingNames.map((name) => firstValueFrom(this.api.getTaskReportFile(requestedTaskId, name))));
      if (this.taskId !== requestedTaskId) {
        return;
      }

      for (const file of files) {
        this.rememberFileContent(file);
      }
    } catch (error) {
      if (this.taskId === requestedTaskId) {
        this.combinedError = this.asErrorMessage(error);
      }
    } finally {
      if (this.taskId === requestedTaskId) {
        this.combinedLoading = false;
      }
    }
  }

  private applySelectedFileContent(file: TaskReportFile): void {
    this.fileContent = file;
    this.editableContent = file.content;
    this.editingFile = false;
    this.lastSavedAt = null;
    this.lastSaveError = '';
    this.refreshSelectedFileArtifacts();
    this.schedulePreviewNavigation();
  }

  private rememberFileContent(file: TaskReportFile): void {
    this.fileContentCache.set(file.name, file);
  }

  private countLines(content: string): number {
    return content === '' ? 1 : content.split(/\r?\n/).length;
  }

  private resolveReportFileName(sourceFile: string): string | null {
    return this.report ? resolveNamedReportFile(this.report.files, sourceFile) : null;
  }

  private matchesCurrentFile(sourceFile: string | null): boolean {
    return this.report ? matchesSelectedFile(this.selectedFileName, this.report.files, sourceFile) : false;
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



































