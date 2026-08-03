import {Idiomorph} from 'idiomorph';
import {errorMessage} from '../modules/errors.ts';
import {request} from '../modules/fetch.ts';
import {showErrorToast} from '../modules/toast.ts';
import {
  IssueLiveSharedWorker,
  type IssueLiveClientMessage,
  type IssueLiveOperation,
  type IssueLiveState,
  type IssueLiveWorkerEvent,
} from '../modules/issue-live-worker.ts';
import {getComboMarkdownEditor} from './comp/ComboMarkdownEditor.ts';
import {issueLiveRefreshEvent} from './comp/ReactionSelector.ts';

const timelineItemIdPattern = /^(?:issue|pull)(?:comment)?-\d+$/;
const pendingOperations = new Map<string, IssueLiveOperation>();
const operationBatches: IssueLiveOperation[][] = [];
const synchronizedStates = new Map<string, IssueLiveState>();
let pendingBaselineStates: IssueLiveState[] | null = null;
let synchronizedBaseline = false;
let operationFrame: number | null = null;
let issueLiveWorker: IssueLiveSharedWorker | null = null;

type TimelineEntry = {
  key: string,
  element: HTMLElement,
};

type MorphOptionsWithCallbacks = {
  morphStyle: 'innerHTML' | 'outerHTML',
  callbacks: {
    beforeNodeMorphed: (oldNode: Node, newNode: Node) => boolean,
  },
};

function issueLiveUrl() {
  const path = window.location.pathname.replace(/\/+$/, '');
  const url = new URL(`${path}/live`, window.location.origin);
  url.protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  return url.href;
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
  const normalizedState = state ?? 'closed';
  indicator.setAttribute('data-state', normalizedState);
  indicator.classList.remove('tw-text-green', 'tw-text-orange', 'tw-text-red');

  if (normalizedState === 'open') {
    indicator.textContent = '●';
    indicator.title = 'Live updates connected';
    indicator.classList.add('tw-text-green');
  } else if (normalizedState === 'error') {
    indicator.textContent = '●';
    indicator.title = 'Live updates connection error';
    indicator.classList.add('tw-text-red');
  } else {
    indicator.textContent = '○';
    indicator.title = normalizedState === 'connecting' ? 'Connecting live updates' : 'Reconnecting live updates';
    indicator.classList.add('tw-text-orange');
  }
  indicator.setAttribute('aria-label', indicator.title);
}

function timelineCommentId(element: HTMLElement) {
  if (timelineItemIdPattern.test(element.id)) return element.id;

  for (const child of element.querySelectorAll<HTMLElement>('[id]')) {
    if (timelineItemIdPattern.test(child.id)) return child.id;
  }
  return null;
}

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
    element.setAttribute('data-issue-live-key', key);
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

function timelineReactionState(element: HTMLElement) {
  const parts = [...element.querySelectorAll<HTMLElement>('.bottom-reactions [data-reaction-content]')].map((reaction) => {
    const content = reaction.getAttribute('data-reaction-content') ?? '';
    const reacted = reaction.getAttribute('data-has-reacted') ?? '';
    const count = reaction.querySelector<HTMLElement>('.reaction-count')?.textContent?.trim() ?? '';
    return `${content}:${reacted}:${count}`;
  });
  parts.sort();
  return parts.join('|');
}

function timelineAttachmentState(element: HTMLElement) {
  const links = new Set<string>();
  for (const link of element.querySelectorAll<HTMLAnchorElement>('.dropzone-attachments a[href]')) {
    const href = link.getAttribute('href');
    if (href) links.add(href);
  }
  return [...links].sort().join('|');
}

function initialResumeStates(): IssueLiveState[] {
  const timelineEnd = document.querySelector<HTMLElement>('#timeline-comments-end');
  const commentList = timelineEnd?.closest<HTMLElement>('.comment-list');
  if (!timelineEnd || !commentList) return [];

  const entries = currentTimelineEntries(commentList, timelineEnd);
  return entries.map((entry, index) => ({
    key: entry.key,
    beforeKey: entries[index + 1]?.key,
    contentVersion: entry.element.querySelector<HTMLElement>('.edit-content-zone')?.getAttribute('data-content-version') ?? undefined,
    reactionState: timelineReactionState(entry.element),
    attachmentState: timelineAttachmentState(entry.element),
  }));
}

function currentResumeMessage(): IssueLiveClientMessage {
  return {
    type: 'resume',
    initialized: synchronizedBaseline,
    states: synchronizedBaseline ? synchronizedStates.values().toArray() : initialResumeStates(),
  };
}

function updateSharedWorkerResumeState() {
  issueLiveWorker?.updateResume(currentResumeMessage());
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
  element.setAttribute('data-issue-live-key', operation.key);
  return element;
}

function queuePendingOperation(operation: IssueLiveOperation) {
  pendingOperations.set(operation.key, operation);
}

function morphElement(existing: HTMLElement, incoming: HTMLElement) {
  if (existing.outerHTML === incoming.outerHTML) return;
  const options: MorphOptionsWithCallbacks = {
    morphStyle: 'outerHTML',
    callbacks: {
      // Fomantic stores module state and handlers on these nodes. Preserve
      // their identity; global observers initialize newly inserted controls.
      beforeNodeMorphed: (oldNode: Node) => !(oldNode instanceof HTMLElement && oldNode.matches('.comment-header-right, .ui.dropdown')),
    },
  };
  Idiomorph.morph(existing, incoming, options);
}

function syncBottomReactions(existing: HTMLElement, incoming: HTMLElement) {
  const existingContainer = existing.querySelector<HTMLElement>(':scope > .content.comment-container');
  const incomingContainer = incoming.querySelector<HTMLElement>(':scope > .content.comment-container');
  if (!existingContainer || !incomingContainer) return;

  const existingReactions = existingContainer.querySelector<HTMLElement>(':scope > .bottom-reactions');
  const incomingReactions = incomingContainer.querySelector<HTMLElement>(':scope > .bottom-reactions');

  if (existingReactions && incomingReactions) {
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

function positionTimelineEntry(
  element: HTMLElement,
  beforeKey: string | undefined,
  currentByKey: Map<string, HTMLElement>,
  timelineEnd: HTMLElement,
) {
  const anchor = (beforeKey ? currentByKey.get(beforeKey) : null) ?? timelineEnd;
  if (anchor === element) return;
  if (element.nextElementSibling === anchor) return;
  anchor.before(element);
}

function visibleScrollAnchor(entries: TimelineEntry[]) {
  let fallback: TimelineEntry | null = null;
  for (const entry of entries) {
    const rect = entry.element.getBoundingClientRect();
    if (rect.bottom <= 0) continue;
    fallback ??= entry;
    if (rect.top >= 0) return {element: entry.element, top: rect.top};
  }
  if (!fallback) return null;
  return {element: fallback.element, top: fallback.element.getBoundingClientRect().top};
}

function applyTimelineOperations(operations: IssueLiveOperation[]) {
  const timelineEnd = document.querySelector<HTMLElement>('#timeline-comments-end');
  const commentList = timelineEnd?.closest<HTMLElement>('.comment-list');
  if (!timelineEnd || !commentList) return;

  const current = currentTimelineEntries(commentList, timelineEnd);
  const currentByKey = new Map(current.map((entry) => [entry.key, entry.element]));
  const scrollAnchor = visibleScrollAnchor(current);
  const positionOperations: IssueLiveOperation[] = [];

  for (const operation of operations) {
    const existing = currentByKey.get(operation.key);

    if (operation.action === 'delete') {
      if (existing && isEditingTimelineEntry(existing)) {
        queuePendingOperation(operation);
        continue;
      }
      existing?.remove();
      currentByKey.delete(operation.key);
      synchronizedStates.delete(operation.key);
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
    } else {
      timelineEnd.before(incoming);
      currentByKey.set(operation.key, incoming);
      animateInsertedEntry(incoming);
    }

    positionOperations.push(operation);
    if (operation.hash) {
      synchronizedStates.set(operation.key, {
        key: operation.key,
        beforeKey: operation.beforeKey,
        hash: operation.hash,
      });
    }
  }

  // Position from the end towards the beginning so chains of newly inserted
  // entries always have their next anchor in the document already.
  for (const operation of [...positionOperations].reverse()) {
    const element = currentByKey.get(operation.key);
    if (element) positionTimelineEntry(element, operation.beforeKey, currentByKey, timelineEnd);
  }

  if (scrollAnchor?.element.isConnected) {
    const offset = scrollAnchor.element.getBoundingClientRect().top - scrollAnchor.top;
    if (offset) window.scrollBy(0, offset);
  }
  updateSharedWorkerResumeState();
}

function applyPendingBaseline() {
  if (!pendingBaselineStates || operationFrame !== null || operationBatches.length) return;

  for (const state of pendingBaselineStates) synchronizedStates.set(state.key, state);
  pendingBaselineStates = null;
  synchronizedBaseline = true;
  updateSharedWorkerResumeState();
}

function flushOperationBatches() {
  operationFrame = null;
  const operations = operationBatches.shift();
  if (operations) applyTimelineOperations(operations);
  if (operationBatches.length) {
    operationFrame = window.requestAnimationFrame(flushOperationBatches);
  } else {
    applyPendingBaseline();
  }
}

function enqueueTimelineOperations(operations: IssueLiveOperation[]) {
  if (!operations.length) return;
  operationBatches.push(operations);
  if (operationFrame === null) operationFrame = window.requestAnimationFrame(flushOperationBatches);
}

function enqueueBaseline(states: IssueLiveState[]) {
  pendingBaselineStates = states;
  applyPendingBaseline();
}

function flushPendingOperations() {
  if (!pendingOperations.size) return;

  const timelineEnd = document.querySelector<HTMLElement>('#timeline-comments-end');
  const commentList = timelineEnd?.closest<HTMLElement>('.comment-list');
  if (!timelineEnd || !commentList) return;
  const currentByKey = new Map(currentTimelineEntries(commentList, timelineEnd).map((entry) => [entry.key, entry.element]));
  const ready: IssueLiveOperation[] = [];

  for (const [key, operation] of pendingOperations) {
    const existing = currentByKey.get(key);
    if (existing && isEditingTimelineEntry(existing)) continue;
    pendingOperations.delete(key);
    ready.push(operation);
  }
  enqueueTimelineOperations(ready);
}

function initPendingOperationObserver() {
  const commentList = document.querySelector<HTMLElement>('.repository.view.issue .comment-list');
  if (!commentList) return;

  const observer = new MutationObserver(flushPendingOperations);
  observer.observe(commentList, {attributes: true, subtree: true, attributeFilter: ['class']});
}

function initLiveCommentSubmit(worker: IssueLiveSharedWorker) {
  const form = document.querySelector<HTMLFormElement>('#comment-form');
  if (!form) return;

  form.addEventListener('submit', async (event: SubmitEvent) => {
    if (event.defaultPrevented) return;
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
      let responseJson: {errorMessage?: string} | null = null;
      try {
        responseJson = await response.json();
      } catch {
        // Non-JSON error responses fall back to the HTTP status text.
      }
      if (!response.ok) {
        throw new Error(responseJson?.errorMessage || `Unable to create comment: ${response.statusText}`);
      }

      const editor = getComboMarkdownEditor(form.querySelector('.combo-markdown-editor'));
      editor?.value('');
      editor?.textarea.dispatchEvent(new Event('input', {bubbles: true}));
      editor?.dropzoneResetFiles();
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

  const worker = new IssueLiveSharedWorker(issueLiveUrl(), currentResumeMessage());
  issueLiveWorker = worker;
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
    } else if (message.type === 'baseline') {
      enqueueBaseline(message.states ?? []);
    } else if (message.type === 'timeline' && message.operations) {
      if ((message.sequence ?? 0) <= latestSequence) return;
      latestSequence = message.sequence ?? latestSequence + 1;
      enqueueTimelineOperations(message.operations);
    } else if (message.type === 'worker-error') {
      console.error(message.message);
    }
  });
  worker.startPort();
  worker.status();
  initLiveCommentSubmit(worker);
  initPendingOperationObserver();

  const refreshFromPageAction = () => worker.refresh();
  document.addEventListener(issueLiveRefreshEvent, refreshFromPageAction);

  const flushVisiblePendingOperations = () => {
    if (document.visibilityState !== 'visible') return;
    flushPendingOperations();
    updateSharedWorkerResumeState();
    worker.check();
  };
  document.addEventListener('visibilitychange', flushVisiblePendingOperations);

  window.addEventListener('pagehide', () => {
    document.removeEventListener(issueLiveRefreshEvent, refreshFromPageAction);
    document.removeEventListener('visibilitychange', flushVisiblePendingOperations);
    issueLiveWorker = null;
    worker.close();
  }, {once: true});
}
