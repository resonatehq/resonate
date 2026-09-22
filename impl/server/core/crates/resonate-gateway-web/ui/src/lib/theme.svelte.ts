import { browser } from '$app/environment';

const KEY = 'resonate.console.theme';
export type Theme = 'light' | 'dark';

function initial(): Theme {
  if (!browser) return 'light';
  return document.documentElement.dataset.theme === 'dark' ? 'dark' : 'light';
}

export const theme = $state<{ value: Theme }>({ value: initial() });

/** Flip `data-theme` on the root, and remember which way. */
export function toggleTheme() {
  theme.value = theme.value === 'dark' ? 'light' : 'dark';
  if (!browser) return;
  document.documentElement.dataset.theme = theme.value;
  try {
    localStorage.setItem(KEY, theme.value);
  } catch {
    /* a browser that refuses storage still themes for this tab */
  }
}
