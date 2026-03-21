import { Routes } from '@angular/router';

import { DashboardPageComponent } from './dashboard-page.component';
import { ReportPageComponent } from './report-page.component';

export const routes: Routes = [
  { path: '', component: DashboardPageComponent },
  { path: 'report', component: ReportPageComponent },
  { path: '**', redirectTo: '' },
];
