import {Idiomorph} from 'idiomorph';
import {errorMessage} from '../modules/errors.ts';
import {request} from '../modules/fetch.ts';
import {showErrorToast} from '../modules/toast.ts';
import {
  IssueLiveSharedWorker,
  type IssueLiveOperation,
  type IssueLiveWorkerEvent,
} from '../modules/issue-live-worker.ts';
import {getComboMarkdownEditor} from './comp/ComboMarkdownEditor.ts';
import {issueLiveRefreshEvent} from './comp/ReactionSelector.ts';

const timelineItemIdPattern = /^(?:issue|pull)(?:comment)?-\d+$/;
const pendingOperations = new Map<string, IssueLiveOperation>();
let pendingOperationsTimer: number | null = null;

type TimelineEntry = {
  key: string,
  element: HTMLElement,
};

function issueLiveUrl() {
  const path = window.location.pathname.replace(/\/+$/, '');
  const url = new URL(`${path}/content-history/overview`, window.location.origin);
  url.protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  return url.toString();
}

function ensureConnectionIndicator() {
  let indicator = document.querySelector<HTMLElement>('#issue-live-connection-status');
  if (indicator) return indicator;

  indicator = document.createElement('span');
  indicator.id = 'issue-live-connection-status';
  indicator.className = 'tw-float-right tw-p-1 tw-text-xs tw-text-text-light';
  indicator.setAttribute('role', 'status');
  indicator.setAttribute('aria-live', 'polite');
  document.querySelector('#timeline-comments-end')?.before(indicator);
  return indicator;
}

function updateConnectionIndicator(state: IssueLiveWorkerEvent['state']) {
  const indicator = ensureConnectionIndicator();
  indicator.dataset.state = state ?? 'closed';
  indicator.classList.remove('tw-text-green', 'tw-text-orange', 'tw-text-red');

  if (state === 'open') {
    indicator.textContent = '●';
    indicator.title = 'Live updates connected';
    indicator.setAttribute('aria-label', indicator.title);
    indicator.classList.add('tw-text-green');
  } else if (state === 'error') {
    indicator.textContent = '●';
    indicator.title = 'Live updates connection error';
    indicator.setAttribute('aria-label', indicator.title);
    indicator.classList.add('tw-text-red');
  } else {
    indicator.textContent = '○';
    indicator.title = state === 'connecting' ? 'Connecting live updates' : 'Reconnecting live updates';
    indicator.setAttribute('aria-label', indicator.title);
    indicator.classList.add('tw-text-orange');
  }
}

function timelineCommentId(element: HTMLElement) {
  if (timelineItemIdPattern.test(element.id)) return element.id;

  for (const child of element.querySelectorAll<HTMLElement>('[id]')) {
    if (timelineItemIdPattern.test(child.id)) return child.id;
  }
  return null;
}

/**
 * Build stable, top-level timeline entries. Some Gitea activities render more
 * than one node (for example a push plus its commit list), while review events
 * render a timeline-item-group whose ID is on a child.
 */
function collectTimelineEntries(elements: Iterable<HTMLElement>): TimelineEntry[] {
  const result: TimelineEntry[] = [];
  let previousCommentKey: string | null = null;
  let unkeyedIndex = 0;

  for (const element of elements) {
    let key = timelineCommentId(element);
    if (key) {
      previousCommentKey = key;
    } else if (previousCommentKey && element.matches('.timeline-item.commits-list')) {
      key = `${previousCommentKey}:commits`;
    } else if (element.matches('.timeline-item, .timeline-item-group')) {
      key = `unkeyed:${unkeyedIndex}`;
      unkeyedIndex += 1;
    }

    if (!key) continue;
    element.dataset.issueLiveKey = key;
    result.push({key, element});
  }
  return result;
}

function currentTimelineEntries(commentList: HTMLElement, timelineEnd: HTMLElement) {
  const elements: HTMLElement[] = [];
  for (const element of commentList.children) {
    if (element === timelineEnd) break;
    if (element instanceof HTMLElement) elements.push(element);
  }
  return collectTimelineEntries(elements);
}

function isEditingTimelineEntry(element: HTMLElement) {
  for (const editZone of element.querySelectorAll<HTMLElement>('.edit-content-zone')) {
    if (!editZone.classList.contains('tw-hidden')) return true;
  }
  return false;
}

function animateInsertedEntry(element: HTMLElement) {
  if (window.matchMedia('(prefers-reduced-motion: reduce)').matches) return;
  element.animate([{opacity: 0}, {opacity: 1}], {duration: 180, easing: 'ease-out'});
}

function parseOperationElement(operation: IssueLiveOperation) {
  if (!operation.html) return null;

  const template = document.createElement('template');
  template.innerHTML = operation.html;
  const element = template.content.firstElementChild;
  if (!(element instanceof HTMLElement)) return null;
  element.dataset.issueLiveKey = operation.key;
  return element;
}

function queuePendingOperation(operation: IssueLiveOperation) {
  pendingOperations.set(operation.key, operation);
  if (pendingOperationsTimer !== null) return;

  pendingOperationsTimer = window.setTimeout(() => {
    pendingOperationsTimer = null;
    const operations = Array.from(pendingOperations.values());
    pendingOperations.clear();
    applyTimelineOperations(operations);
  }, 500);
}

function morphElement(existing: HTMLElement, incoming: HTMLElement) {
  if (existing.outerHTML === incoming.outerHTML) return;
  Idiomorph.morph(existing, incoming, {
    morphStyle: 'outerHTML',
    callbacks: {
      // Fomantic keeps its module state and event handlers on these live nodes.
      // Never morph their DOM identity; newly inserted dropdowns are initialized
      // by Gitea's global MutationObserver.
      beforeNodeMorphed(oldNode) {
        return !(oldNode instanceof HTMLElement && oldNode.matches('.comment-header-right, .ui.dropdown'));
      },
    },
  });
}

function syncBottomReactions(existing: HTMLElement, incoming: HTMLElement) {
  const existingContainer = existing.querySelector<HTMLElement>(':scope > .content.comment-container');
  const incomingContainer = incoming.querySelector<HTMLElement>(':scope > .content.comment-container');
  if (!existingContainer || !incomingContainer) return;

  const existingReactions = existingContainer.querySelector<HTMLElement>(':scope > .bottom-reactions');
  const incomingReactions = incomingContainer.querySelector<HTMLElement>(':scope > .bottom-reactions');

  if (existingReactions && incomingReactions) {
    // Replacing this small fragment is intentional: the reaction buttons and
    // tooltips are dynamic, and the newly inserted dropdown is safely
    // initialized by the global selector observer.
    existingReactions.replaceWith(incomingReactions);
  } else if (existingReactions) {
    existingReactions.remove();
  } else if (incomingReactions) {
    existingContainer.append(incomingReactions);
  }
}

function morphCommentEntry(existing: HTMLElement, incoming: HTMLElement) {
  const existingHeaderLeft = existing.querySelector<HTMLElement>('.comment-header-left');
  const incomingHeaderLeft = incoming.querySelector<HTMLElement>('.comment-header-left');
  if (existingHeaderLeft && incomingHeaderLeft) morphElement(existingHeaderLeft, incomingHeaderLeft);

  const existingBody = existing.querySelector<HTMLElement>('.comment-body');
  const incomingBody = incoming.querySelector<HTMLElement>('.comment-body');
  if (existingBody && incomingBody) morphElement(existingBody, incomingBody);

  syncBottomReactions(existing, incoming);
}

function morphTimelineEntry(existing: HTMLElement, incoming: HTMLElement) {
  if (existing.matches('.timeline-item.comment') && incoming.matches('.timeline-item.comment')) {
    morphCommentEntry(existing, incoming);
    return;
  }
  morphElement(existing, incoming);
}

function applyTimelineOperations(operations: IssueLiveOperation[]) {
  const timelineEnd = document.querySelector<HTMLElement>('#timeline-comments-end');
  const commentList = timelineEnd?.closest<HTMLElement>('.comment-list');
  if (!timelineEnd || !commentList) return;

  const current = currentTimelineEntries(commentList, timelineEnd);
  const currentByKey = new Map(current.map((entry) => [entry.key, entry.element]));

  for (const operation of operations) {
    const existing = currentByKey.get(operation.key);

    if (operation.action === 'delete') {
      if (!existing) continue;
      if (isEditingTimelineEntry(existing)) {
        queuePendingOperation(operation);
        continue;
      }
      existing.remove();
      currentByKey.delete(operation.key);
      continue;
    }

    const incoming = parseOperationElement(operation);
    if (!incoming) continue;

    if (existing) {
      if (isEditingTimelineEntry(existing)) {
        queuePendingOperation(operation);
        continue;
      }
      morphTimelineEntry(existing, incoming);
      continue;
    }

    const anchor = operation.beforeKey ? currentByKey.get(operation.beforeKey) : null;
    (anchor?.isConnected ? anchor : timelineEnd).before(incoming);
    currentByKey.set(operation.key, incoming);
    animateInsertedEntry(incoming);
  }
}

function initLiveCommentSubmit(worker: IssueLiveSharedWorker) {
  const form = document.querySelector<HTMLFormElement>('#comment-form');
  if (!form) return;

  form.addEventListener('submit', async (event: SubmitEvent) => {
    const submitter = event.submitter as HTMLButtonElement | null;
    if (submitter?.id === 'status-button') return;

    event.preventDefault();
    event.stopImmediatePropagation();

    const formData = new FormData(form);
    if (submitter?.name) formData.append(submitter.name, submitter.value);

    submitter?.setAttribute('disabled', '');
    form.classList.add('is-loading');
    try {
      const headers = new Headers({'X-Gitea-Fetch-Action': '1'});
      const response = await request(form.action, {
        method: form.method || 'POST',
        body: formData,
        headers,
      });
      const responseJson = await response.json().catch(() => null);
      if (!response.ok) {
        throw new Error(responseJson?.errorMessage || `Unable to create comment: ${response.statusText}`);
      }

      const editor = getComboMarkdownEditor(form.querySelector('.combo-markdown-editor'));
      editor?.value('');
      editor?.textarea.dispatchEvent(new Event('input', {bubbles: true}));
      editor?.dropzoneReloadFiles();
      worker.refresh();
    } catch (error) {
      console.error(error);
      showErrorToast(errorMessage(error));
    } finally {
      submitter?.removeAttribute('disabled');
      form.classList.remove('is-loading');
    }
  }, {capture: true});
}

export function initRepoIssueLive() {
  if (!document.querySelector('.repository.view.issue .comment-list')) return;
  if (!window.WebSocket || !window.SharedWorker) return;

  const worker = new IssueLiveSharedWorker(issueLiveUrl());
  let latestSequence = 0;
  let connectionState: IssueLiveWorkerEvent['state'];

  worker.addMessageEventListener((event: MessageEvent<IssueLiveWorkerEvent>) => {
    const message = event.data;
    if (message.type === 'connection-status') {
      if (message.state === 'connecting' || (message.state === 'open' && connectionState !== 'open')) {
        // Server sequences are scoped to one WebSocket connection and restart
        // after reconnecting.
        latestSequence = 0;
      }
      connectionState = message.state;
      updateConnectionIndicator(message.state);
    } else if (message.type === 'timeline' && message.operations) {
      if ((message.sequence ?? 0) <= latestSequence) return;
      latestSequence = message.sequence ?? latestSequence + 1;
      applyTimelineOperations(message.operations);
    } else if (message.type === 'worker-error') {
      console.error(message.message);
    }
  });
  worker.startPort();
  worker.status();
  initLiveCommentSubmit(worker);

  const refreshFromPageAction = () => worker.refresh();
  document.addEventListener(issueLiveRefreshEvent, refreshFromPageAction);

  window.addEventListener('pagehide', () => {
    document.removeEventListener(issueLiveRefreshEvent, refreshFromPageAction);
    worker.close();
  }, {once: true});
}
