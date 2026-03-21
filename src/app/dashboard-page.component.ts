import { CommonModule } from '@angular/common';
import { Component, DestroyRef, OnInit, inject } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { FormBuilder, ReactiveFormsModule } from '@angular/forms';
import { Router } from '@angular/router';
import { debounceTime, finalize, firstValueFrom } from 'rxjs';

import { DashboardApiService } from './dashboard-api.service';
import { BatchOption, ConversationRow, HealthResponse, TaskFilters, TeamMember } from './models';

type SortKey = 'taskId' | 'turnCount' | 'complexity' | 'batch' | 'schemaName' | 'businessStatus' | 'assignedUser';
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
    taskIdQuery: '',
    status: 'all',
    batchId: DashboardPageComponent.DEFAULT_BATCH_ID,
  });

  batchOptions: BatchOption[] = [];
  teamMembers: TeamMember[] = [];
  taskIdOptions: string[] = [];
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
    this.filtersForm.valueChanges.pipe(debounceTime(250), takeUntilDestroyed(this.destroyRef)).subscribe(() => {
      this.fetchData();
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

    const rawFilters = this.filtersForm.getRawValue() as TaskFilters;
    const filters = {
      ...rawFilters,
      userId: this.resolveUserFilter(rawFilters.userId),
    };

    this.api
      .getConversations(filters)
      .pipe(finalize(() => (this.loading = false)))
      .subscribe({
        next: (rows) => {
          this.rows = this.applyTaskIdFilter(rows, filters.taskIdQuery);
          this.updateDisplayRows();
          this.taskIdOptions = this.uniqueTaskIds(this.rows);
          this.message = this.rows.length
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

  fetchTask(row: ConversationRow): void {
    if (!row.taskId || this.hasBackgroundTaskProcess) {
      return;
    }

    this.loadingTaskId = row.taskId;
    this.error = '';
    this.message = '';

    this.api
      .fetchTaskOutput(row.taskId, this.buildTaskFetchPayload(row))
      .pipe(finalize(() => (this.loadingTaskId = null)))
      .subscribe({
        next: (result) => {
          const generatedCount = result.generatedFiles.length;
          const graphqlNote = result.graphqlErrors ? ` GraphQL returned ${result.graphqlErrors} error(s).` : '';
          const metadataNote = result.metadataFile ? ` Metadata file ${result.metadataFile} is ready.` : '';
          const schemaNote = result.schemaFile
            ? ` Schema cache ${result.schemaFile} is ready.`
            : result.schemaError
              ? ` Schema generation failed: ${result.schemaError}`
              : '';
          this.message = `Fetched task ${result.taskId} and generated ${generatedCount} file${generatedCount === 1 ? '' : 's'} in ${result.folderPath}.${graphqlNote}${metadataNote}${schemaNote}`;
        },
        error: (error: unknown) => {
          this.error = this.asErrorMessage(error);
        },
      });
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
          await firstValueFrom(this.api.fetchTaskOutput(row.taskId, this.buildTaskFetchPayload(row)));
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

  isFetchRunningForTask(taskId: string): boolean {
    return this.loadingTaskId === taskId || this.bulkFetchTaskId === taskId;
  }

  isValidateRunningForTask(taskId: string): boolean {
    return this.validatingTaskId === taskId || this.bulkValidateTaskId === taskId;
  }

  openReport(taskId: string): void {
    void this.router.navigate(['/report'], {
      queryParams: { taskId },
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
      error: (error: unknown) => {
        this.error = this.asErrorMessage(error);
      },
    });
  }

  private loadBatches(): void {
    this.api.getBatches().subscribe({
      next: (batches) => {
        this.batchOptions = batches;
      },
      error: (error: unknown) => {
        this.error = this.asErrorMessage(error);
      },
    });
  }

  private applyTaskIdFilter(rows: ConversationRow[], taskIdQuery: string): ConversationRow[] {
    const query = taskIdQuery.trim().toLowerCase();

    if (!query) {
      return rows;
    }

    return rows.filter((row) => row.taskId.toLowerCase().includes(query));
  }

  private uniqueTaskIds(rows: ConversationRow[]): string[] {
    return [...new Set(rows.map((row) => row.taskId))].sort((left, right) => left.localeCompare(right));
  }

  private updateDisplayRows(): void {
    this.displayRows = this.sortRows(this.rows, this.sortState);
  }

  private buildTaskFetchPayload(row: ConversationRow): { promptId?: string; collabLink?: string; metadata: unknown } {
    return {
      promptId: row.promptId?.trim() || undefined,
      collabLink: this.getCollabHref(row) ?? undefined,
      metadata: row.metadata,
    };
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






