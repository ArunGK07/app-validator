import { CommonModule } from '@angular/common';
import { Component, DestroyRef, OnInit, inject } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { FormBuilder, ReactiveFormsModule } from '@angular/forms';
import { Router } from '@angular/router';
import { debounceTime, finalize, firstValueFrom } from 'rxjs';

import { DashboardApiService } from './dashboard-api.service';
import { BatchOption, ConversationRow, HealthResponse, TaskFilters, TeamMember } from './models';

type SortKey = 'taskId' | 'turnCount' | 'complexity' | 'batch' | 'schemaName' | 'businessStatus' | 'assignedUser' | 'lastReviewScore';
type SortDirection = 'asc' | 'desc';

interface SortState {
  key: SortKey;
  direction: SortDirection;
}

@Component({
  selector: 'app-dashboard-page',
  standalone: true,
  imports: [CommonModule, ReactiveFormsModule],
  templateUrl: './dashboard-page.component.html',
  styleUrl: './dashboard-page.component.css',
})
export class DashboardPageComponent implements OnInit {
  private static readonly DEFAULT_BATCH_ID = '311';
  private static readonly FILTERS_SESSION_KEY = 'app-validator.dashboard.filters';
  private readonly api = inject(DashboardApiService);
  private readonly destroyRef = inject(DestroyRef);
  private readonly fb = inject(FormBuilder);
  private readonly router = inject(Router);
  private readonly labelingBaseUrl = 'https://labeling-o.turing.com';
  private readonly rlhfBaseUrl = 'https://rlhf-v3.turing.com';

  readonly statusOptions = [
    { value: 'all', label: 'All' },
    { value: 'in-progress', label: 'In Progress' },
    { value: 'rework', label: 'Rework' },
    { value: 'completed', label: 'Completed' },
    { value: 'reviewed-once', label: 'Reviewed Once' },
  ];
  readonly filtersForm = this.fb.nonNullable.group({
    userId: '',
    status: 'all',
    batchId: DashboardPageComponent.DEFAULT_BATCH_ID,
    taskOutputOnly: false,
  });
  readonly taskLookupControl = this.fb.nonNullable.control('');
  readonly columnFiltersForm = this.fb.nonNullable.group({
    batch: '',
    taskId: '',
    turnCount: '',
    complexity: '',
    businessStatus: '',
    assignedUser: '',
  });

  batchOptions: BatchOption[] = [];
  teamMembers: TeamMember[] = [];
  rows: ConversationRow[] = [];
  displayRows: ConversationRow[] = [];
  loading = false;
  message = '';
  error = '';
  health: HealthResponse | null = null;
  openMetadataTaskId: string | null = null;
  sortState: SortState | null = null;
  loadingTaskId: string | null = null;
  bulkFetchInProgress = false;
  bulkFetchTaskId: string | null = null;
  bulkFetchCompleted = 0;
  validatingTaskId: string | null = null;
  bulkValidateInProgress = false;
  bulkValidateTaskId: string | null = null;
  bulkValidateCompleted = 0;

  ngOnInit(): void {
    this.restoreFiltersFromSession();

    this.filtersForm.valueChanges.pipe(takeUntilDestroyed(this.destroyRef)).subscribe(() => {
      this.persistFiltersToSession();
    });

    this.filtersForm.valueChanges.pipe(debounceTime(250), takeUntilDestroyed(this.destroyRef)).subscribe(() => {
      this.fetchData();
    });

    this.columnFiltersForm.valueChanges.pipe(debounceTime(150), takeUntilDestroyed(this.destroyRef)).subscribe(() => {
      this.updateDisplayRows();
    });

    this.loadHealth();
    this.loadBatches();
    this.loadTeamMembers();
  }

  fetchData(): void {
    this.loading = true;
    this.loadingTaskId = null;
    this.error = '';
    this.message = '';
    this.openMetadataTaskId = null;

    const rawFilters = this.filtersForm.getRawValue();
    const taskOutputOnly = rawFilters.taskOutputOnly;
    const filters = {
      taskIdQuery: '',
      status: rawFilters.status,
      batchId: rawFilters.batchId,
      userId: this.resolveUserFilter(rawFilters.userId),
    } satisfies TaskFilters;

    const request$ = taskOutputOnly ? this.api.getTaskOutputTasks() : this.api.getConversations(filters);

    request$
      .pipe(finalize(() => (this.loading = false)))
      .subscribe({
        next: (rows) => {
          this.rows = rows;
          this.updateDisplayRows();
          this.message = taskOutputOnly
            ? this.rows.length
              ? `Loaded ${this.rows.length} task-output folder${this.rows.length === 1 ? '' : 's'}.`
              : 'No task folders were found in task-output.'
            : this.rows.length
              ? `Loaded ${this.rows.length} conversation row${this.rows.length === 1 ? '' : 's'}.`
              : 'No rows matched the current filters.';
        },
        error: (error: unknown) => {
          this.error = this.asErrorMessage(error);
        },
      });
  }

  toggleMetadata(taskId: string): void {
    this.openMetadataTaskId = this.openMetadataTaskId === taskId ? null : taskId;
  }

  async fetchTask(row: ConversationRow): Promise<void> {
    if (!row.taskId || this.hasBackgroundTaskProcess) {
      return;
    }

    const taskId = row.taskId;
    this.loadingTaskId = taskId;
    this.error = '';
    this.message = '';

    try {
      const freshRow = await firstValueFrom(this.api.getConversation(taskId));
      this.replaceRow(freshRow);
      const result = await firstValueFrom(this.api.fetchTaskOutput(freshRow.taskId, this.buildTaskFetchPayload(freshRow)));
      const generatedCount = result.generatedFiles.length;
      const graphqlNote = result.graphqlErrors ? ` GraphQL returned ${result.graphqlErrors} error(s).` : '';
      const metadataNote = result.metadataFile ? ` Metadata file ${result.metadataFile} is ready.` : '';
      const schemaNote = result.schemaFile
        ? ` Schema cache ${result.schemaFile} is ready.`
        : result.schemaError
          ? ` Schema generation failed: ${result.schemaError}`
          : '';
      this.message = `Fetched task ${result.taskId} and generated ${generatedCount} file${generatedCount === 1 ? '' : 's'} in ${result.folderPath}.${graphqlNote}${metadataNote}${schemaNote}`;
    } catch (error) {
      this.error = this.asErrorMessage(error);
    } finally {
      if (this.loadingTaskId === taskId) {
        this.loadingTaskId = null;
      }
    }
  }

  async fetchAllTasks(): Promise<void> {
    if (!this.displayRows.length || this.hasBackgroundTaskProcess) {
      return;
    }

    this.bulkFetchInProgress = true;
    this.bulkFetchTaskId = null;
    this.bulkFetchCompleted = 0;
    this.error = '';
    this.message = `Fetching ${this.displayRows.length} task${this.displayRows.length === 1 ? '' : 's'} from the current table.`;

    const failures = [];

    try {
      for (const row of this.displayRows) {
        this.bulkFetchTaskId = row.taskId;
        this.bulkFetchCompleted += 1;
        this.message = `Fetching ${this.bulkFetchCompleted} of ${this.displayRows.length}: task ${row.taskId}.`;

        try {
          const freshRow = await firstValueFrom(this.api.getConversation(row.taskId));
          this.replaceRow(freshRow);
          await firstValueFrom(this.api.fetchTaskOutput(freshRow.taskId, this.buildTaskFetchPayload(freshRow)));
        } catch (error) {
          failures.push({
            taskId: row.taskId,
            message: this.asErrorMessage(error),
          });
        }
      }

      if (failures.length) {
        const failedTaskIds = failures.map((failure) => failure.taskId).join(', ');
        this.error = `${failures.length} task fetch${failures.length === 1 ? '' : 'es'} failed: ${failedTaskIds}.`;
      }

      const successCount = this.displayRows.length - failures.length;
      this.message = `Fetched ${successCount} of ${this.displayRows.length} task${this.displayRows.length === 1 ? '' : 's'} from the table.`;
    } finally {
      this.bulkFetchInProgress = false;
      this.bulkFetchTaskId = null;
      this.bulkFetchCompleted = 0;
    }
  }

  validateTask(row: ConversationRow): void {
    if (!row.taskId || this.hasBackgroundTaskProcess) {
      return;
    }

    this.validatingTaskId = row.taskId;
    this.error = '';
    this.message = `Validation started for task ${row.taskId}.`;

    this.api
      .runTaskWorkflowAction(row.taskId, 'validate')
      .pipe(finalize(() => (this.validatingTaskId = null)))
      .subscribe({
        next: (result) => {
          const failedChecks = this.readValidationFailedChecks(result);
          const passedChecks = this.readValidationPassedChecks(result);
          this.message = result.success
            ? `Validated task ${result.taskId}: ${passedChecks} checks passed.`
            : `Validation finished for task ${result.taskId} with ${failedChecks} failing check${failedChecks === 1 ? '' : 's'}.`;

          if (!result.success) {
            this.error = `Validation failed for task ${result.taskId}. Check ${result.logFile}.`;
          }
        },
        error: (error: unknown) => {
          this.error = this.asErrorMessage(error);
        },
      });
  }

  async validateAllTasks(): Promise<void> {
    if (!this.displayRows.length || this.hasBackgroundTaskProcess) {
      return;
    }

    this.bulkValidateInProgress = true;
    this.bulkValidateTaskId = null;
    this.bulkValidateCompleted = 0;
    this.error = '';
    this.message = `Validating ${this.displayRows.length} task${this.displayRows.length === 1 ? '' : 's'} from the current table.`;

    const failures: Array<{ taskId: string; message: string }> = [];

    try {
      for (const row of this.displayRows) {
        this.bulkValidateTaskId = row.taskId;
        this.bulkValidateCompleted += 1;
        this.message = `Validating ${this.bulkValidateCompleted} of ${this.displayRows.length}: task ${row.taskId}.`;

        try {
          const result = await firstValueFrom(this.api.runTaskWorkflowAction(row.taskId, 'validate'));

          if (!result.success) {
            failures.push({
              taskId: row.taskId,
              message: `${this.readValidationFailedChecks(result)} checks failed.`,
            });
          }
        } catch (error) {
          failures.push({
            taskId: row.taskId,
            message: this.asErrorMessage(error),
          });
        }
      }

      if (failures.length) {
        const failedTaskIds = failures.map((failure) => failure.taskId).join(', ');
        this.error = `${failures.length} task validation${failures.length === 1 ? '' : 's'} failed: ${failedTaskIds}.`;
      }

      const successCount = this.displayRows.length - failures.length;
      this.message = `Validated ${successCount} of ${this.displayRows.length} task${this.displayRows.length === 1 ? '' : 's'} from the table.`;
    } finally {
      this.bulkValidateInProgress = false;
      this.bulkValidateTaskId = null;
      this.bulkValidateCompleted = 0;
    }
  }

  isMetadataOpen(taskId: string): boolean {
    return this.openMetadataTaskId === taskId;
  }

  sortBy(key: SortKey): void {
    if (!this.sortState || this.sortState.key !== key) {
      this.sortState = { key, direction: 'asc' };
    } else {
      this.sortState = {
        key,
        direction: this.sortState.direction === 'asc' ? 'desc' : 'asc',
      };
    }

    this.updateDisplayRows();
  }

  getAriaSort(key: SortKey): 'ascending' | 'descending' | 'none' {
    if (this.sortState?.key !== key) {
      return 'none';
    }

    return this.sortState.direction === 'asc' ? 'ascending' : 'descending';
  }

  isSortActive(key: SortKey): boolean {
    return this.sortState?.key === key;
  }

  isSortDescending(key: SortKey): boolean {
    return this.sortState?.key === key && this.sortState.direction === 'desc';
  }

  trackByTaskId(_index: number, row: ConversationRow): string {
    return row.taskId;
  }

  get showTaskOutputOnly(): boolean {
    return this.filtersForm.controls.taskOutputOnly.getRawValue();
  }

  getCollabHref(row: ConversationRow): string | null {
    if (row.collabLink?.trim()) {
      return row.collabLink.trim();
    }

    if (!row.promptId?.trim()) {
      return null;
    }

    const url = new URL(`prompt/${row.promptId.trim()}`, `${this.rlhfBaseUrl}/`);
    url.searchParams.set('origin', this.labelingBaseUrl);
    url.searchParams.set('redirect_url', `${this.labelingBaseUrl}/conversations/${row.taskId}/view`);
    return url.toString();
  }

  isTaskActionDisabled(): boolean {
    return this.loading || this.hasBackgroundTaskProcess;
  }

  isTaskLookupDisabled(): boolean {
    return !this.taskLookupControl.getRawValue().trim() || this.hasBackgroundTaskProcess;
  }

  isFetchRunningForTask(taskId: string): boolean {
    return this.loadingTaskId === taskId || this.bulkFetchTaskId === taskId;
  }

  isValidateRunningForTask(taskId: string): boolean {
    return this.validatingTaskId === taskId || this.bulkValidateTaskId === taskId;
  }

  openReport(taskId: string): void {
    this.persistFiltersToSession();

    void this.router.navigate(['/report'], {
      queryParams: { taskId },
    });
  }

  openReview(taskId: string): void {
    this.persistFiltersToSession();

    void this.router.navigate(['/review'], {
      queryParams: { taskId },
    });
  }

  openTaskLookup(): void {
    const taskId = this.taskLookupControl.getRawValue().trim();
    if (!taskId) {
      this.error = 'Enter a task id to open the task page.';
      this.message = '';
      return;
    }

    this.persistFiltersToSession();
    this.error = '';
    this.message = '';

    void this.router.navigate(['/report'], {
      queryParams: { taskId, fetch: Date.now().toString() },
    });
  }

  private loadHealth(): void {
    this.api.getHealth().subscribe({
      next: (health) => {
        this.health = health;
      },
      error: () => {
        this.health = {
          configured: false,
          message: 'Backend health check failed. Confirm the proxy server is running.',
        };
      },
    });
  }

  private loadTeamMembers(): void {
    this.api.getTeamMembers().subscribe({
      next: (teamMembers) => {
        this.teamMembers = teamMembers;
        this.fetchData();
      },
      error: () => {
        // Secondary loader — empty list is acceptable; avoid overwriting the main error
      },
    });
  }

  private loadBatches(): void {
    this.api.getBatches().subscribe({
      next: (batches) => {
        this.batchOptions = batches;
      },
      error: () => {
        // Secondary loader — empty list is acceptable; avoid overwriting the main error
      },
    });
  }

  get hasColumnFilters(): boolean {
    return Object.values(this.columnFiltersForm.getRawValue()).some((v) => v.trim());
  }

  get uniqueBatches(): string[] {
    return [...new Set(this.rows.map((r) => r.batch).filter(Boolean))].sort();
  }

  get uniqueTurnCounts(): string[] {
    return [...new Set(this.rows.map((r) => r.turnCount).filter(Boolean))]
      .sort((a, b) => Number(a) - Number(b));
  }

  get uniqueComplexities(): string[] {
    const order = ['easy', 'medium', 'hard', 'unknown'];
    return [...new Set(this.rows.map((r) => r.complexity).filter(Boolean))]
      .sort((a, b) => {
        const ai = order.indexOf(a.toLowerCase());
        const bi = order.indexOf(b.toLowerCase());
        return (ai === -1 ? 99 : ai) - (bi === -1 ? 99 : bi);
      });
  }

  get uniqueBusinessStatuses(): string[] {
    return [...new Set(this.rows.map((r) => r.businessStatus).filter(Boolean))].sort();
  }

  get uniqueAssignedUsers(): string[] {
    return [...new Set(this.rows.map((r) => r.assignedUser).filter(Boolean))].sort();
  }

  clearColumnFilters(): void {
    this.columnFiltersForm.reset();
  }

  private filterRows(rows: ConversationRow[]): ConversationRow[] {
    const f = this.columnFiltersForm.getRawValue();
    const batch = f.batch.trim();
    const taskId = f.taskId.trim().toLowerCase();
    const turnCount = f.turnCount.trim();
    const complexity = f.complexity.trim().toLowerCase();
    const businessStatus = f.businessStatus.trim();
    const assignedUser = f.assignedUser.trim();

    if (!batch && !taskId && !turnCount && !complexity && !businessStatus && !assignedUser) {
      return rows;
    }

    return rows.filter((row) =>
      (!batch || row.batch === batch) &&
      (!taskId || row.taskId.toLowerCase().includes(taskId)) &&
      (!turnCount || row.turnCount === turnCount) &&
      (!complexity || row.complexity.toLowerCase() === complexity) &&
      (!businessStatus || row.businessStatus === businessStatus) &&
      (!assignedUser || row.assignedUser === assignedUser),
    );
  }

  private updateDisplayRows(): void {
    this.displayRows = this.sortRows(this.filterRows(this.rows), this.sortState);
  }

  private buildTaskFetchPayload(row: ConversationRow): { promptId?: string; collabLink?: string; metadata: unknown } {
    return {
      promptId: row.promptId?.trim() || undefined,
      collabLink: this.getCollabHref(row) ?? undefined,
      metadata: row.metadata,
    };
  }

  private replaceRow(updatedRow: ConversationRow): void {
    const rowIndex = this.rows.findIndex((entry) => entry.taskId === updatedRow.taskId);

    if (rowIndex === -1) {
      return;
    }

    this.rows = [
      ...this.rows.slice(0, rowIndex),
      updatedRow,
      ...this.rows.slice(rowIndex + 1),
    ];
    this.updateDisplayRows();
  }

  private sortRows(rows: ConversationRow[], sortState: SortState | null): ConversationRow[] {
    if (!sortState) {
      return [...rows];
    }

    const direction = sortState.direction === 'asc' ? 1 : -1;

    return rows
      .map((row, index) => ({ row, index }))
      .sort((left, right) => {
        const comparison = this.compareRows(left.row, right.row, sortState.key);

        if (comparison !== 0) {
          return comparison * direction;
        }

        return left.index - right.index;
      })
      .map(({ row }) => row);
  }

  private compareRows(left: ConversationRow, right: ConversationRow, key: SortKey): number {
    switch (key) {
      case 'taskId':
        return this.compareText(left.taskId, right.taskId);
      case 'turnCount':
        return this.compareNumber(left.turnCount, right.turnCount);
      case 'complexity':
        return this.compareComplexity(left.complexity, right.complexity);
      case 'batch':
        return this.compareText(left.batch, right.batch);
      case 'schemaName':
        return this.compareText(left.schemaName, right.schemaName);
      case 'businessStatus':
        return this.compareText(left.businessStatus, right.businessStatus);
      case 'assignedUser':
        return this.compareText(left.assignedUser, right.assignedUser);
      case 'lastReviewScore': {
        const ls = left.lastReviewScore ?? -1;
        const rs = right.lastReviewScore ?? -1;
        return ls - rs;
      }
    }
  }

  private compareNumber(left: string, right: string): number {
    const leftValue = Number(left);
    const rightValue = Number(right);

    if (Number.isNaN(leftValue) || Number.isNaN(rightValue)) {
      return this.compareText(left, right);
    }

    return leftValue - rightValue;
  }

  private compareComplexity(left: string, right: string): number {
    const rank = new Map<string, number>([
      ['unknown', 0],
      ['easy', 1],
      ['medium', 2],
      ['hard', 3],
    ]);

    const leftRank = rank.get(left.trim().toLowerCase()) ?? Number.MAX_SAFE_INTEGER;
    const rightRank = rank.get(right.trim().toLowerCase()) ?? Number.MAX_SAFE_INTEGER;

    if (leftRank !== rightRank) {
      return leftRank - rightRank;
    }

    return this.compareText(left, right);
  }

  private compareText(left: string, right: string): number {
    return left.localeCompare(right, undefined, { numeric: true, sensitivity: 'base' });
  }

  private resolveUserFilter(selectedUserId: string): string {
    if (selectedUserId) {
      return selectedUserId;
    }

    return this.teamMembers.map((member) => member.id).filter(Boolean).join(',');
  }

  private restoreFiltersFromSession(): void {
    const savedState = this.readFiltersFromSession();
    if (!savedState) {
      return;
    }

    this.filtersForm.patchValue(savedState.filters, { emitEvent: false });
    this.taskLookupControl.setValue(savedState.taskLookupValue, { emitEvent: false });
  }

  private persistFiltersToSession(): void {
    try {
      sessionStorage.setItem(
        DashboardPageComponent.FILTERS_SESSION_KEY,
        JSON.stringify({
          filters: this.filtersForm.getRawValue(),
          taskLookupValue: this.taskLookupControl.getRawValue(),
        }),
      );
    } catch {
      // Ignore session storage failures and keep the dashboard usable.
    }
  }

  private readFiltersFromSession(): {
    filters: { userId: string; status: string; batchId: string; taskOutputOnly: boolean };
    taskLookupValue: string;
  } | null {
    try {
      const storedValue = sessionStorage.getItem(DashboardPageComponent.FILTERS_SESSION_KEY);
      if (!storedValue) {
        return null;
      }

      const parsed = JSON.parse(storedValue);
      if (!parsed || typeof parsed !== 'object') {
        return null;
      }

      const stored = parsed as {
        filters?: Partial<TaskFilters> & { taskOutputOnly?: boolean };
        taskLookupValue?: string;
        userId?: string;
        status?: string;
        batchId?: string;
        taskOutputOnly?: boolean;
      };
      const filterSource = stored.filters && typeof stored.filters === 'object' ? stored.filters : stored;

      return {
        filters: {
          userId: typeof filterSource.userId === 'string' ? filterSource.userId : '',
          status: typeof filterSource.status === 'string' && filterSource.status ? filterSource.status : 'all',
          batchId:
            typeof filterSource.batchId === 'string' && filterSource.batchId
              ? filterSource.batchId
              : DashboardPageComponent.DEFAULT_BATCH_ID,
          taskOutputOnly: Boolean(filterSource.taskOutputOnly),
        },
        taskLookupValue: typeof stored.taskLookupValue === 'string' ? stored.taskLookupValue : '',
      };
    } catch {
      return null;
    }
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

    return 'Something went wrong while calling the proxy.';
  }

  private get hasBackgroundTaskProcess(): boolean {
    return Boolean(
      this.loadingTaskId ||
        this.validatingTaskId ||
        this.bulkFetchInProgress ||
        this.bulkValidateInProgress,
    );
  }

  private readValidationFailedChecks(result: { summary?: unknown }): number {
    if (result.summary && typeof result.summary === 'object' && 'itemsFailed' in result.summary) {
      const value = (result.summary as { itemsFailed?: unknown }).itemsFailed;
      return typeof value === 'number' ? value : 0;
    }

    return 0;
  }

  private readValidationPassedChecks(result: { summary?: unknown }): number {
    if (result.summary && typeof result.summary === 'object' && 'itemsPassed' in result.summary) {
      const value = (result.summary as { itemsPassed?: unknown }).itemsPassed;
      return typeof value === 'number' ? value : 0;
    }

    return 0;
  }
}








