import { redirect } from '@sveltejs/kit';
import { resolve } from '$app/paths';

/** The landing screen is the executions list. */
export function load() {
  redirect(307, resolve('/executions'));
}
