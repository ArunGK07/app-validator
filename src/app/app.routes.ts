import { Routes } from '@angular/router';

import { DashboardPageComponent } from './dashboard-page.component';
import { ReportPageComponent } from './report-page.component';
import { ReviewPageComponent } from './review-page.component';

export const routes: Routes = [
  { path: '', component: DashboardPageComponent },
  { path: 'report', component: ReportPageComponent },
  { path: 'review', component: ReviewPageComponent },
  { path: '**', redirectTo: '' },
];
