import { CommonModule } from '@angular/common';
import { AfterViewInit, Component, ElementRef, OnDestroy, inject, ViewChild } from '@angular/core';
import { NavigationEnd, Router, RouterLink, RouterLinkActive, RouterOutlet } from '@angular/router';
import { Subscription } from 'rxjs';

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [CommonModule, RouterOutlet, RouterLink, RouterLinkActive],
  templateUrl: './app.component.html',
  styleUrl: './app.component.css',
})
export class AppComponent implements AfterViewInit, OnDestroy {
  private readonly router = inject(Router);

  @ViewChild('shell', { static: true }) private shellRef?: ElementRef<HTMLElement>;
  @ViewChild('topbar') private topbarRef?: ElementRef<HTMLElement>;

  private resizeObserver?: ResizeObserver;
  private routerSubscription?: Subscription;
  showTopbar = true;

  ngAfterViewInit(): void {
    this.routerSubscription = this.router.events.subscribe((event) => {
      if (event instanceof NavigationEnd) {
        this.syncRouteChrome();
      }
    });

    this.syncRouteChrome();
  }

  ngOnDestroy(): void {
    this.resizeObserver?.disconnect();
    this.routerSubscription?.unsubscribe();
  }

  private syncRouteChrome(): void {
    this.showTopbar = !this.router.url.startsWith('/report');
    queueMicrotask(() => this.updateTopbarOffset());
  }

  private updateTopbarOffset(): void {
    const shell = this.shellRef?.nativeElement;
    const topbar = this.topbarRef?.nativeElement;

    this.resizeObserver?.disconnect();

    if (!shell) {
      return;
    }

    if (!this.showTopbar || !topbar) {
      shell.style.setProperty('--app-topbar-height', '0px');
      shell.style.setProperty('--app-topbar-offset', '0px');
      return;
    }

    const topbarHeight = Math.ceil(topbar.getBoundingClientRect().height);
    const stickyOffset = topbarHeight + 32;

    shell.style.setProperty('--app-topbar-height', `${topbarHeight}px`);
    shell.style.setProperty('--app-topbar-offset', `${stickyOffset}px`);

    if (typeof ResizeObserver === 'undefined') {
      return;
    }

    this.resizeObserver = new ResizeObserver(() => this.updateTopbarOffset());
    this.resizeObserver.observe(topbar);
  }
}

