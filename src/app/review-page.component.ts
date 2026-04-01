import { CommonModule, DecimalPipe } from '@angular/common';
import { Component, OnInit, inject } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';

import { DashboardApiService } from './dashboard-api.service';
import { ReviewDetail } from './models';

@Component({
  selector: 'app-review-page',
  standalone: true,
  imports: [CommonModule, DecimalPipe],
  templateUrl: './review-page.component.html',
  styleUrl: './review-page.component.css',
})
export class ReviewPageComponent implements OnInit {
  private readonly api = inject(DashboardApiService);
  private readonly route = inject(ActivatedRoute);
  private readonly router = inject(Router);

  taskId = '';
  review: ReviewDetail | null = null;
  loading = false;
  error = '';
  qdSortField: 'grade' | 'score' | null = null;
  qdSortDir: 'asc' | 'desc' = 'asc';

  ngOnInit(): void {
    this.route.queryParamMap.subscribe((params) => {
      const taskId = params.get('taskId') ?? '';
      this.taskId = taskId;
      if (taskId) {
        this.loadReview(taskId);
      } else {
        this.error = 'No task ID provided.';
      }
    });
  }

  private loadReview(taskId: string): void {
    this.loading = true;
    this.error = '';
    this.review = null;

    this.api.getConversationReview(taskId).subscribe({
      next: (detail) => {
        this.review = detail;
        this.loading = false;
      },
      error: (err: unknown) => {
        this.error = this.asErrorMessage(err);
        this.loading = false;
      },
    });
  }

  goBack(): void {
    void this.router.navigate(['/']);
  }

  get labelingHref(): string {
    return `https://labeling-o.turing.com/conversations/${this.taskId}/view`;
  }

  get scoreClass(): string {
    const score = this.review?.score;
    if (score == null) {
      return '';
    }
    if (score >= 80) {
      return 'score--good';
    }
    if (score >= 50) {
      return 'score--warn';
    }
    return 'score--bad';
  }

  get auditEntries(): { key: string; value: string }[] {
    const audit = this.review?.audit;
    if (!audit || typeof audit !== 'object' || Array.isArray(audit)) {
      return [];
    }
    return Object.entries(audit as Record<string, unknown>).map(([key, value]) => ({
      key,
      value: typeof value === 'string' ? value : JSON.stringify(value),
    }));
  }

  sortQdBy(field: 'grade' | 'score'): void {
    if (this.qdSortField === field) {
      this.qdSortDir = this.qdSortDir === 'asc' ? 'desc' : 'asc';
    } else {
      this.qdSortField = field;
      this.qdSortDir = 'asc';
    }
  }

  get sortedQualityDimensions() {
    const dims = this.review?.qualityDimensions ?? [];
    if (!this.qdSortField) return dims;
    const dir = this.qdSortDir === 'asc' ? 1 : -1;
    return [...dims].sort((a, b) => {
      if (this.qdSortField === 'grade') {
        const order = ['Pass', 'Fail', '—'];
        const ai = order.indexOf(a.scoreText ?? '—');
        const bi = order.indexOf(b.scoreText ?? '—');
        return dir * ((ai === -1 ? 99 : ai) - (bi === -1 ? 99 : bi));
      }
      if (this.qdSortField === 'score') {
        const av = a.score ?? -1;
        const bv = b.score ?? -1;
        return dir * (av - bv);
      }
      return 0;
    });
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
    return 'Failed to load review details.';
  }
}
