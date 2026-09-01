<script lang="ts">
  import '../app.css';
  import { base } from '$app/paths';
  import { page } from '$app/state';
  import { shell } from '$lib/shell.svelte';
  import { theme, toggleTheme } from '$lib/theme.svelte';

  let { children } = $props();

  /**
   * Which rail button is lit. The detail route belongs to executions — an
   * operator who drilled into one has not left the section.
   */
  const section = $derived.by(() => {
    const p = page.url.pathname;
    if (p.startsWith(`${base}/schedules`)) return 'schedules';
    if (p.startsWith(`${base}/settings`)) return 'settings';
    return 'executions';
  });
</script>

<div class="app">
  <aside class="rail">
    <div class="mark" title="Resonate">
      <svg width="22" height="22" viewBox="0 0 211.66664 211.66664" fill="currentColor" aria-hidden="true">
        <path
          d="M 106.32838,18.377687 A 97.327225,87.388214 0 0 0 9.0015166,105.76614 97.327225,87.388214 0 0 0 106.32838,193.15408 97.327225,87.388214 0 0 0 203.65576,105.76614 97.327225,87.388214 0 0 0 106.32838,18.377687 Z m 0.23771,15.456419 a 80.729752,72.485672 0 0 1 80.72995,72.485494 80.729752,72.485672 0 0 1 -80.72995,72.486 80.729752,72.485672 0 0 1 -80.729944,-72.486 80.729752,72.485672 0 0 1 80.729944,-72.485494 z"
        />
        <ellipse cx="62.486992" cy="101.5191" rx="23.858042" ry="27.060209" />
        <ellipse cx="149.49829" cy="101.05645" rx="23.858042" ry="27.060209" />
        <ellipse cx="102.61576" cy="129.12817" rx="2.9793823" ry="4.7031751" />
        <ellipse cx="111.29089" cy="129.16696" rx="2.9793823" ry="4.7031751" />
      </svg>
    </div>

    <nav>
      <!-- Lucide (MIT): workflow, clock, settings. Three SVGs, not a package. -->
      <a
        href="{base}/executions"
        title="Durable Executions"
        class="navbtn"
        class:on={section === 'executions'}
        aria-current={section === 'executions' ? 'page' : undefined}
      >
        <svg width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
          <rect width="8" height="8" x="3" y="3" rx="2" />
          <path d="M7 11v4a2 2 0 0 0 2 2h4" />
          <rect width="8" height="8" x="13" y="13" rx="2" />
        </svg>
      </a>
      <a
        href="{base}/schedules"
        title="Schedules"
        class="navbtn"
        class:on={section === 'schedules'}
        aria-current={section === 'schedules' ? 'page' : undefined}
      >
        <svg width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
          <circle cx="12" cy="12" r="9.2" />
          <path d="M12 6.6V12l3.6 1.9" />
        </svg>
      </a>
      <a
        href="{base}/settings"
        title="Settings"
        class="navbtn"
        class:on={section === 'settings'}
        aria-current={section === 'settings' ? 'page' : undefined}
      >
        <svg width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
          <path d="M9.671 4.136a2.34 2.34 0 0 1 4.659 0 2.34 2.34 0 0 0 3.319 1.915 2.34 2.34 0 0 1 2.33 4.033 2.34 2.34 0 0 0 0 3.831 2.34 2.34 0 0 1-2.33 4.033 2.34 2.34 0 0 0-3.319 1.915 2.34 2.34 0 0 1-4.659 0 2.34 2.34 0 0 0-3.32-1.915 2.34 2.34 0 0 1-2.33-4.033 2.34 2.34 0 0 0 0-3.831A2.34 2.34 0 0 1 6.35 6.051a2.34 2.34 0 0 0 3.319-1.915" />
          <circle cx="12" cy="12" r="3" />
        </svg>
      </a>
    </nav>

    <div class="spacer"></div>

    <button
      class="navbtn"
      onclick={toggleTheme}
      title={theme.value === 'dark' ? 'Dark' : 'Light'}
      aria-label="Toggle theme"
    >
      <span class="half"></span>
    </button>
  </aside>

  <main>
    <header>
      <div class="titles">
        <h1>{shell.title}</h1>
        {#if shell.sub}<div class="sub">{shell.sub}</div>{/if}
      </div>
      {#if shell.searchable}
        <div class="search">
          <span class="lens"></span>
          <input
            bind:value={shell.query}
            placeholder="Search execution id, function, tag…"
            spellcheck="false"
            autocomplete="off"
            aria-label="Search"
          />
        </div>
      {/if}
      {#if shell.refresh}
        <button class="btn" onclick={() => shell.refresh?.()} disabled={shell.busy}>
          {shell.busy ? 'Refreshing…' : 'Refresh'}
        </button>
      {/if}
      {#if shell.action}
        <button class="btn-accent" onclick={() => shell.action?.run()}>{shell.action.label}</button>
      {/if}
    </header>

    <div class="content" id="console-content">
      {@render children()}
    </div>
  </main>
</div>

<style>
  .app {
    display: flex;
    height: 100vh;
    min-height: 640px;
    background: var(--bg);
    color: var(--text);
  }

  .rail {
    width: 58px;
    flex: none;
    border-right: 1px solid var(--line);
    background: var(--panel);
    display: flex;
    flex-direction: column;
    align-items: center;
    padding: 14px 0 12px;
    gap: 6px;
  }
  .mark {
    width: 34px;
    height: 34px;
    display: flex;
    align-items: center;
    justify-content: center;
    color: var(--text);
    margin-bottom: 8px;
  }
  nav {
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 4px;
  }
  .spacer {
    flex: 1;
  }
  .navbtn {
    display: flex;
    align-items: center;
    justify-content: center;
    width: 34px;
    height: 34px;
    border: 0;
    border-radius: 7px;
    cursor: pointer;
    background: transparent;
    color: var(--faint);
  }
  .navbtn:hover {
    background: var(--elev);
    color: var(--text);
  }
  .navbtn.on {
    background: var(--elev);
    color: var(--text);
  }
  .half {
    width: 13px;
    height: 13px;
    border-radius: 50%;
    border: 1.5px solid currentColor;
    background: linear-gradient(90deg, currentColor 0 50%, transparent 50% 100%);
  }

  main {
    flex: 1;
    min-width: 0;
    display: flex;
    flex-direction: column;
    overflow: hidden;
  }

  header {
    flex: none;
    border-bottom: 1px solid var(--line);
    padding: 16px 26px;
    display: flex;
    align-items: center;
    gap: 18px;
    background: var(--panel);
  }
  .titles {
    min-width: 0;
    flex: 1;
  }
  h1 {
    margin: 0;
    font-weight: 600;
    font-size: 22px;
    letter-spacing: -0.015em;
    line-height: 1.2;
    /* An id as a page title is still a heading, so it is still Inter. */
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .sub {
    font-size: 12.5px;
    color: var(--faint);
    margin-top: 3px;
  }

  .search {
    display: flex;
    align-items: center;
    gap: 9px;
    border: 1px solid var(--line);
    border-radius: 6px;
    padding: 6px 10px;
    background: var(--elev);
    width: 300px;
    flex: none;
  }
  .search:focus-within {
    border-color: var(--line2);
  }
  .lens {
    width: 11px;
    height: 11px;
    border: 1.4px solid var(--faint);
    border-radius: 50%;
    flex: none;
  }
  .search input {
    flex: 1;
    min-width: 0;
    border: 0;
    background: transparent;
    color: var(--text);
    font-size: 13px;
  }

  .content {
    flex: 1;
    overflow: auto;
  }

  @media (max-width: 860px) {
    header {
      flex-wrap: wrap;
      gap: 10px;
      padding: 12px 14px;
    }
    .search {
      width: 100%;
      order: 3;
    }
  }
</style>
