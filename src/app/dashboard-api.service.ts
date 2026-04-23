import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable, inject } from '@angular/core';
import { Observable, catchError, map, throwError } from 'rxjs';

import {
  BatchOption,
  ConversationRow,
  HealthResponse,
  ReviewDetail,
  TaskFetchResult,
  TaskFilters,
  TaskReport,
  TaskReportFile,
  TaskWorkflowAction,
  TaskWorkflowActionResult,
  TeamMember,
} from './models';

@Injectable({ providedIn: 'root' })
export class DashboardApiService {
  private readonly http = inject(HttpClient);

  getHealth(): Observable<HealthResponse> {
    return this.http.get<HealthResponse>('/api/health');
  }

  getTeamMembers(): Observable<TeamMember[]> {
    return this.http.get<TeamMember[]>('/api/team-members');
  }

  getBatches(): Observable<BatchOption[]> {
    return this.http.get<BatchOption[]>('/api/batches');
  }

  getTaskReport(taskId: string): Observable<TaskReport> {
    return this.http.get<TaskReport>(`/api/reports/${encodeURIComponent(taskId)}`);
  }

  getConversation(taskId: string): Observable<ConversationRow> {
    const requestedTaskId = taskId.trim();

    return this.http.get<ConversationRow>(`/api/conversations/${encodeURIComponent(taskId)}`).pipe(
      catchError((error: unknown) => {
        const status = typeof error === 'object' && error !== null && 'status' in error
          ? (error as { status?: number }).status
          : undefined;

        if (status !== 404) {
          return throwError(() => error);
        }

        const params = new HttpParams().set('taskId', requestedTaskId);
        return this.http.get<ConversationRow[]>('/api/conversations', { params }).pipe(
          map((rows) => this.pickConversationRow(rows, requestedTaskId, error)),
          catchError(() =>
            this.getTaskOutputTasks(requestedTaskId).pipe(
              map((rows) => this.pickConversationRow(rows, requestedTaskId, error)),
            ),
          ),
        );
      }),
    );
  }

  getConversationReview(taskId: string): Observable<ReviewDetail> {
    return this.http.get<ReviewDetail>(`/api/conversations/${encodeURIComponent(taskId)}/review`);
  }

  getTaskOutputTasks(taskId = ''): Observable<ConversationRow[]> {
    let params = new HttpParams();

    if (taskId.trim()) {
      params = params.set('taskId', taskId.trim());
    }

    return this.http.get<ConversationRow[]>('/api/task-output/tasks', { params });
  }

  editConversation(taskId: string, reason = 'Fixing client feedback'): Observable<unknown> {
    return this.http.post<unknown>(`/api/conversations/${encodeURIComponent(taskId)}/edit`, { reason });
  }

  getTaskReportFile(taskId: string, name: string): Observable<TaskReportFile> {
    const params = new HttpParams().set('name', name);
    return this.http.get<TaskReportFile>(`/api/reports/${encodeURIComponent(taskId)}/file`, { params });
  }

  saveTaskReportFile(taskId: string, name: string, content: string): Observable<TaskReportFile> {
    const params = new HttpParams().set('name', name);
    return this.http.put<TaskReportFile>(`/api/reports/${encodeURIComponent(taskId)}/file`, { content }, { params });
  }

  runTaskWorkflowAction(taskId: string, action: TaskWorkflowAction, options?: { forcePublish?: boolean; autoCommit?: boolean }): Observable<TaskWorkflowActionResult> {
    return this.http.post<TaskWorkflowActionResult>(
      `/api/tasks/${encodeURIComponent(taskId)}/actions/${encodeURIComponent(action)}`,
      { forcePublish: Boolean(options?.forcePublish), autoCommit: Boolean(options?.autoCommit) },
    );
  }

  fetchTaskOutput(
    taskId: string,
    payload: { promptId?: string; collabLink?: string | null; metadata?: unknown },
  ): Observable<TaskFetchResult> {
    return this.http.post<TaskFetchResult>(`/api/tasks/${encodeURIComponent(taskId)}/fetch-output`, payload);
  }

  getConversations(filters: TaskFilters): Observable<ConversationRow[]> {
    let params = new HttpParams();

    if (filters.userId) {
      params = params.set('userId', filters.userId);
    }

    if (filters.taskIdQuery) {
      params = params.set('taskId', filters.taskIdQuery.trim());
    }

    if (filters.status) {
      params = params.set('status', filters.status);
    }

    if (filters.batchId) {
      params = params.set('batchId', filters.batchId);
    }

    return this.http.get<ConversationRow[]>('/api/conversations', { params });
  }

  private pickConversationRow(rows: ConversationRow[], taskId: string, fallbackError: unknown): ConversationRow {
    const row = rows.find((entry) => entry.taskId === taskId) ?? rows[0];

    if (!row) {
      throw fallbackError;
    }

    return row;
  }

  checkImportJsonStatus(taskId: string): Observable<{ taskId: string; hasImportJson: boolean }> {
    return this.http.get<{ taskId: string; hasImportJson: boolean }>
      (`/api/tasks/${encodeURIComponent(taskId)}/import-json-status`);
  }
}

