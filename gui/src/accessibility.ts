import type { KeyboardEvent as ReactKeyboardEvent } from 'react';

/**
 * Standard horizontal tab-list keyboard support. Arrow keys, Home, and End
 * move focus and activate the target tab so keyboard and pointer users get the
 * same view. Enter and Space are handled by the buttons themselves.
 */
export function handleTabKey(event: ReactKeyboardEvent<HTMLButtonElement>): void {
  if (!['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(event.key)) return;
  const list = event.currentTarget.closest('[role="tablist"]');
  if (!list) return;
  const tabs = Array.from(list.querySelectorAll<HTMLButtonElement>('[role="tab"]')).filter(
    (tab) => !tab.disabled,
  );
  if (tabs.length === 0) return;
  const current = tabs.indexOf(event.currentTarget);
  let next = current;
  if (event.key === 'ArrowLeft') next = (current - 1 + tabs.length) % tabs.length;
  if (event.key === 'ArrowRight') next = (current + 1) % tabs.length;
  if (event.key === 'Home') next = 0;
  if (event.key === 'End') next = tabs.length - 1;
  event.preventDefault();
  tabs[next].focus();
  tabs[next].click();
}
