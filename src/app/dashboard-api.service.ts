import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable, inject } from '@angular/core';
import { Observable } from 'rxjs';

import {
  BatchOption,
  ConversationRow,
  HealthResponse,
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

  getTaskReportFile(taskId: string, name: string): Observable<TaskReportFile> {
    const params = new HttpParams().set('name', name);
    return this.http.get<TaskReportFile>(`/api/reports/${encodeURIComponent(taskId)}/file`, { params });
  }

  saveTaskReportFile(taskId: string, name: string, content: string): Observable<TaskReportFile> {
    const params = new HttpParams().set('name', name);
    return this.http.put<TaskReportFile>(`/api/reports/${encodeURIComponent(taskId)}/file`, { content }, { params });
  }

  runTaskWorkflowAction(taskId: string, action: TaskWorkflowAction): Observable<TaskWorkflowActionResult> {
    return this.http.post<TaskWorkflowActionResult>(`/api/tasks/${encodeURIComponent(taskId)}/actions/${encodeURIComponent(action)}`, {});
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
}
