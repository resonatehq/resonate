<script lang="ts">
  import '../app.css';
  import { asset, resolve } from '$app/paths';
  import { page } from '$app/state';
  import { shell } from '#lib/shell.svelte.ts';
  import { theme, toggleTheme } from '#lib/theme.svelte.ts';

  let { children } = $props();

  /**
   * Which rail button is lit. The detail route belongs to executions — an
   * operator who drilled into one has not left the section.
   */
  const section = $derived.by(() => {
    const p = page.url.pathname;
    if (p.startsWith(resolve('/schedules'))) return 'schedules';
    if (p.startsWith(resolve('/settings'))) return 'settings';
    return 'executions';
  });
</script>

<svelte:head>
  <title>{shell.title} · Resonate Console</title>
</svelte:head>

<a class="skip" href="#console-content">Skip to console content</a>
<div class="app">
  <div class="brandbar">
    <a class="identity" href={resolve('/executions')} aria-label="Resonate console home">
      <img class="echo-mark" src={asset('images/resonate/echo.png')} width="29" height="26" alt="" />
      <span>Resonate</span>
    </a>
    <span class="console-label">Developer console</span>
  </div>
  <aside class="rail">
    <div class="overline">Workspace</div>

    <nav aria-label="Console navigation">
      <!-- Lucide (MIT): workflow, clock, settings. Three SVGs, not a package. -->
      <a
        href={resolve('/executions')}
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
        <span>Executions</span>
      </a>
      <a
        href={resolve('/schedules')}
        title="Schedules"
        class="navbtn"
        class:on={section === 'schedules'}
        aria-current={section === 'schedules' ? 'page' : undefined}
      >
        <svg width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
          <circle cx="12" cy="12" r="9.2" />
          <path d="M12 6.6V12l3.6 1.9" />
        </svg>
        <span>Schedules</span>
      </a>
      <a
        href={resolve('/settings')}
        title="Settings"
        class="navbtn"
        class:on={section === 'settings'}
        aria-current={section === 'settings' ? 'page' : undefined}
      >
        <svg width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
          <path d="M9.671 4.136a2.34 2.34 0 0 1 4.659 0 2.34 2.34 0 0 0 3.319 1.915 2.34 2.34 0 0 1 2.33 4.033 2.34 2.34 0 0 0 0 3.831 2.34 2.34 0 0 1-2.33 4.033 2.34 2.34 0 0 0-3.319 1.915 2.34 2.34 0 0 1-4.659 0 2.34 2.34 0 0 0-3.32-1.915 2.34 2.34 0 0 1-2.33-4.033 2.34 2.34 0 0 0 0-3.831A2.34 2.34 0 0 1 6.35 6.051a2.34 2.34 0 0 0 3.319-1.915" />
          <circle cx="12" cy="12" r="3" />
        </svg>
        <span>Settings</span>
      </a>
    </nav>

    <div class="spacer"></div>

    <div class="rail-foot">
      <img class="echo-mark" src={asset('images/resonate/echo.png')} width="42" height="42" alt="" />
      <div class="overline">Durable execution</div>
    </div>
    <button
      class="navbtn theme"
      onclick={toggleTheme}
      title={theme.value === 'dark' ? 'Switch to light theme' : 'Switch to dark theme'}
      aria-label={theme.value === 'dark' ? 'Switch to light theme' : 'Switch to dark theme'}
    >
      <span class="half"></span>
      <span>{theme.value === 'dark' ? 'Dark' : 'Light'} theme</span>
    </button>
  </aside>

  <main>
    <header>
      <div class="titles">
        <div class="overline">Console / {section === 'executions' ? 'Executions' : section === 'schedules' ? 'Schedules' : 'Settings'}</div>
        <h1 title={shell.title}>{shell.title}</h1>
        {#if shell.sub}<div class="sub">{shell.sub}</div>{/if}
      </div>
      {#if shell.refresh}
        <button class="btn" onclick={() => shell.refresh?.()} disabled={shell.busy}>
          {shell.busy ? 'Refreshing…' : 'Refresh'}
        </button>
      {/if}
      {#if shell.action}
        <button class="btn-accent" onclick={() => shell.action?.run()}>{shell.action.label}</button>
      {/if}
    </header>
    {#if shell.searchable}
      <div class="searchbar">
        <div class="search">
          <span class="lens" aria-hidden="true"></span>
          <input
            bind:value={shell.query}
            placeholder={section === 'schedules' ? 'Search schedule id, cron, promise id, tag…' : 'Search execution id, function, tag…'}
            spellcheck="false"
            autocomplete="off"
            aria-label={section === 'schedules' ? 'Search loaded schedules' : 'Search loaded executions'}
          />
        </div>
      </div>
    {/if}

    <div class="content" id="console-content" tabindex="-1">
      {@render children()}
    </div>
  </main>
</div>

<style>
  .app {
    display: grid;
    grid-template-columns: 205px minmax(0, 1fr);
    grid-template-rows: 80px minmax(0, 1fr);
    height: 100dvh;
    min-height: 0;
    background: var(--bg);
    color: var(--text);
  }

  .rail {
    border-right: 1px solid var(--line);
    background: var(--bg);
    display: flex;
    flex-direction: column;
    min-height: 0;
    padding: 30px 18px 22px;
    gap: 6px;
  }
  .brandbar {
    grid-column: 1 / -1;
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 20px;
    padding: 20px 32px;
    border-bottom: 1px solid var(--line);
  }
  .identity {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    color: var(--text);
    font-size: 24px;
    font-weight: 550;
    line-height: 1;
    letter-spacing: -1.4px;
  }
  .console-label {
    color: var(--dim);
    font: 11px var(--mono);
  }
  .overline {
    font: 10px/1.5 var(--mono);
    letter-spacing: .1em;
    text-transform: uppercase;
    color: var(--dim);
  }
  .rail > .overline {
    padding: 0 12px 13px;
  }
  .rail-foot {
    padding: 32px 12px 20px;
  }
  .rail-foot img {
    display: block;
    opacity: .65;
    margin-bottom: 13px;
  }
  .skip {
    position: fixed;
    top: 12px;
    left: 12px;
    z-index: 100;
    padding: 12px 18px;
    background: var(--panel);
    border: 1px solid var(--accent);
    border-radius: 5px;
    transform: translateY(-160%);
  }
  .skip:focus {
    transform: translateY(0);
  }
  nav {
    display: flex;
    flex-direction: column;
    gap: 4px;
  }
  .spacer {
    flex: 1;
  }
  .navbtn {
    display: flex;
    align-items: center;
    gap: 11px;
    width: 100%;
    min-height: 41px;
    padding: 12px;
    border: 0;
    border-radius: 5px;
    cursor: pointer;
    background: transparent;
    color: var(--dim);
    font-size: 13px;
    text-align: left;
  }
  .navbtn:hover {
    background: var(--elev);
    color: var(--text);
  }
  .navbtn.on {
    background: var(--selected);
    color: var(--text);
    font-weight: 600;
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
    min-height: 0;
    display: flex;
    flex-direction: column;
    overflow: hidden;
  }

  header {
    flex: none;
    padding: 35px 26px 24px;
    display: flex;
    align-items: center;
    gap: 18px;
    background: var(--bg);
  }
  .titles {
    min-width: 0;
    flex: 1;
  }
  h1 {
    margin: 0;
    font-weight: 400;
    font-size: 30px;
    letter-spacing: -1.2px;
    line-height: 1.2;
    margin-top: 9px;
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
    padding: 10px 12px;
    background: var(--panel);
    width: min(100%, 540px);
  }
  .search:focus-within {
    outline: 2px solid var(--accent);
    outline-offset: 2px;
  }
  .searchbar {
    padding: 0 26px 4px;
  }
  .search input:focus-visible {
    outline: none;
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
    }
    .searchbar { padding: 0 14px 4px; }
  }
  @media (max-width: 1050px) and (min-width: 768px) {
    .app { grid-template-columns: 170px minmax(0, 1fr); }
    .rail { padding-left: 10px; padding-right: 10px; }
  }
  @media (max-width: 767px) {
    .app {
      grid-template-columns: minmax(0, 1fr);
      grid-template-rows: 73px auto minmax(0, 1fr);
    }
    .brandbar { padding: 18px 17px; }
    .console-label { font-size: 10px; }
    .rail {
      flex-direction: row;
      border-right: 0;
      border-bottom: 1px solid var(--line);
      padding: 8px 12px;
      align-items: center;
    }
    .rail > .overline, .rail-foot, .spacer { display: none; }
    nav { flex-direction: row; flex: 1; gap: 2px; }
    nav .navbtn { justify-content: center; padding: 11px 8px; font-size: 12px; }
    nav :global(svg) { display: none; }
    .theme { width: 40px; justify-content: center; padding: 10px; }
    .theme > span:last-child { display: none; }
    header { padding-top: 25px; }
    .titles { flex-basis: 100%; }
    h1 { font-size: 28px; }
  }
</style>
