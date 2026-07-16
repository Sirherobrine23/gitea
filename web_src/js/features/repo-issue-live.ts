import {Idiomorph} from 'idiomorph';
import {errorMessage} from '../modules/errors.ts';
import {request} from '../modules/fetch.ts';
import {showErrorToast} from '../modules/toast.ts';
import {getComboMarkdownEditor} from './comp/ComboMarkdownEditor.ts';
import {issueLiveRefreshEvent} from './comp/ReactionSelector.ts';

const timelineItemIdPattern = /^(?:issue|pull)(?:comment)?-\d+$/;
const hiddenSocketSuspendDelay = 15000;
const pendingOperations = new Map<string, IssueLiveOperation>();
const operationBatches: IssueLiveOperation[][] = [];
const synchronizedStates = new Map<string, IssueLiveState>();
let pendingBaselineStates: IssueLiveState[] | null = null;
let synchronizedBaseline = false;
let operationFrame: number | null = null;

type ConnectionState = 'connecting' | 'open' | 'closed' | 'error';

type IssueLiveState = {
  key: string,
  beforeKey?: string,
  hash?: string,
  contentVersion?: string,
  reactionState?: string,
  attachmentState?: string,
};

type IssueLiveOperation = {
  action: 'upsert' | 'delete',
  key: string,
  beforeKey?: string,
  html?: string,
  hash?: string,
};

type IssueLiveServerMessage = {
  type: 'timeline' | 'baseline',
  sequence?: number,
  operations?: IssueLiveOperation[],
  states?: IssueLiveState[],
};

type IssueLiveClientMessage = {
  type: 'resume',
  initialized: boolean,
  states: IssueLiveState[],
};

type TimelineEntry = {
  key: string,
  element: HTMLElement,
};

class IssueLiveSocket {
  url: string;
  socket: WebSocket | null = null;
  reconnectTimer: number | null = null;
  reconnectDelay = 1000;
  pendingRefresh = false;
  suspended = false;
  stopped = false;
  onMessage: (message: IssueLiveServerMessage) => void;
  onState: (state: ConnectionState) => void;
  getResumeMessage: () => IssueLiveClientMessage;

  constructor(
    url: string,
    onMessage: (message: IssueLiveServerMessage) => void,
    onState: (state: ConnectionState) => void,
    getResumeMessage: () => IssueLiveClientMessage,
  ) {
    this.url = url;
    this.onMessage = onMessage;
    this.onState = onState;
    this.getResumeMessage = getResumeMessage;
  }

  start() {
    this.stopped = false;
    this.suspended = false;
    this.connect();
  }

  connect() {
    if (this.stopped || this.suspended) return;
    if (this.socket?.readyState === WebSocket.CONNECTING || this.socket?.readyState === WebSocket.OPEN) return;

    this.cancelReconnect();
    this.onState('connecting');
    const socket = new WebSocket(this.url);
    this.socket = socket;

    socket.addEventListener('open', () => {
      if (this.socket !== socket) return;
      try {
        socket.send(JSON.stringify(this.getResumeMessage()));
      } catch (error) {
        console.error('Unable to send issue live resume state', error);
        socket.close(1011, 'Unable to resume');
        return;
      }
      this.reconnectDelay = 1000;
      this.onState('open');
      if (this.pendingRefresh) {
        this.pendingRefresh = false;
        this.refresh();
      }
    });

    socket.addEventListener('message', (event) => {
      if (this.socket !== socket) return;
      try {
        this.onMessage(JSON.parse(String(event.data)) as IssueLiveServerMessage);
      } catch (error) {
        console.error('Unable to decode issue live message', error);
      }
    });

    socket.addEventListener('error', () => {
      if (this.socket === socket) this.onState('error');
    });

    socket.addEventListener('close', () => {
      if (this.socket !== socket) return;
      this.socket = null;
      this.onState('closed');
      this.scheduleReconnect();
    });
  }

  scheduleReconnect() {
    if (this.stopped || this.suspended || this.reconnectTimer !== null) return;
    const jitter = 0.8 + Math.random() * 0.4;
    const delay = Math.round(this.reconnectDelay * jitter);
    this.reconnectDelay = Math.min(this.reconnectDelay * 2, 30000);
    this.reconnectTimer = window.setTimeout(() => {
      this.reconnectTimer = null;
      this.connect();
    }, delay);
  }

  cancelReconnect() {
    if (this.reconnectTimer === null) return;
    window.clearTimeout(this.reconnectTimer);
    this.reconnectTimer = null;
  }

  refresh() {
    if (this.socket?.readyState === WebSocket.OPEN) {
      this.socket.send(JSON.stringify({type: 'refresh'}));
    } else {
      this.pendingRefresh = true;
      this.connect();
    }
  }

  suspend() {
    if (this.suspended) return;
    this.suspended = true;
    this.cancelReconnect();
    const socket = this.socket;
    this.socket = null;
    socket?.close(1000, 'Page hidden');
    this.onState('closed');
  }

  resume() {
    if (this.stopped) return;
    this.suspended = false;
    this.connect();
    this.refresh();
  }

  close() {
    this.stopped = true;
    this.suspended = true;
    this.cancelReconnect();
    const socket = this.socket;
    this.socket = null;
    socket?.close(1000, 'Page closed');
    this.onState('closed');
  }
}

function issueLiveUrl() {
  const path = window.location.pathname.replace(/\/+$/, '');
  const url = new URL(`${path}/live`, window.location.origin);
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

function updateConnectionIndicator(state: ConnectionState) {
  const indicator = ensureConnectionIndicator();
  indicator.dataset.state = state;
  indicator.classList.remove('tw-text-green', 'tw-text-orange', 'tw-text-red');

  if (state === 'open') {
    indicator.textContent = '●';
    indicator.title = 'Live updates connected';
    indicator.classList.add('tw-text-green');
  } else if (state === 'error') {
    indicator.textContent = '●';
    indicator.title = 'Live updates connection error';
    indicator.classList.add('tw-text-red');
  } else {
    indicator.textContent = '○';
    indicator.title = state === 'connecting' ? 'Connecting live updates' : 'Live updates disconnected';
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

  for (const element of elements) {
    let key = timelineCommentId(element);
    if (key) {
      previousCommentKey = key;
    } else if (previousCommentKey && element.matches('.timeline-item.commits-list')) {
      key = `${previousCommentKey}:commits`;
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

function timelineReactionState(element: HTMLElement) {
  const parts = [...element.querySelectorAll<HTMLElement>('.bottom-reactions [data-reaction-content]')].map((reaction) => {
    const content = reaction.dataset.reactionContent ?? '';
    const reacted = reaction.dataset.hasReacted ?? '';
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
    contentVersion: entry.element.querySelector<HTMLElement>('.edit-content-zone')?.dataset.contentVersion,
    reactionState: timelineReactionState(entry.element),
    attachmentState: timelineAttachmentState(entry.element),
  }));
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
}

function morphElement(existing: HTMLElement, incoming: HTMLElement) {
  if (existing.outerHTML === incoming.outerHTML) return;
  Idiomorph.morph(existing, incoming, {
    morphStyle: 'outerHTML',
    callbacks: {
      // Fomantic stores module state and handlers on these nodes. Preserve
      // their identity; global observers initialize newly inserted controls.
      beforeNodeMorphed(oldNode: Node) {
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
}
function applyPendingBaseline() {
  if (!pendingBaselineStates || operationFrame !== null || operationBatches.length) return;

  synchronizedStates.clear();
  for (const state of pendingBaselineStates) synchronizedStates.set(state.key, state);
  pendingBaselineStates = null;
  synchronizedBaseline = true;
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

function initLiveCommentSubmit(socket: IssueLiveSocket) {
  const form = document.querySelector<HTMLFormElement>('#comment-form');
  if (!form) return;

  form.addEventListener('submit', async (event: SubmitEvent) => {
    if (event.defaultPrevented) return;
    const submitter = event.submitter as HTMLButtonElement | null;
    if (submitter?.id === 'status-button') return;

    event.preventDefault();
    // The form also has Gitea's delegated .form-fetch-action handler. Stop the
    // event before it reaches document or the comment would be submitted twice.
    event.stopPropagation();

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
      socket.refresh();
    } catch (error) {
      console.error(error);
      showErrorToast(errorMessage(error));
    } finally {
      submitter?.removeAttribute('disabled');
      form.classList.remove('is-loading');
    }
  });
}

export function initRepoIssueLive() {
  if (!document.querySelector('.repository.view.issue .comment-list')) return;
  if (!window.WebSocket) return;

  let latestSequence = 0;
  let connectionState: ConnectionState = 'closed';
  const socket = new IssueLiveSocket(
    issueLiveUrl(),
    (message) => {
      if (message.type === 'baseline') {
        // The server sends the baseline after the initial operations. Defer
        // committing it until those DOM operations have run in animation order.
        enqueueBaseline(message.states ?? []);
        return;
      }
      if (message.type !== 'timeline' || !message.operations) return;
      if ((message.sequence ?? 0) <= latestSequence) return;
      latestSequence = message.sequence ?? latestSequence + 1;
      enqueueTimelineOperations(message.operations);
    },
    (state) => {
      if (state === 'connecting' || (state === 'open' && connectionState !== 'open')) {
        // Sequence numbers are scoped to one WebSocket connection.
        latestSequence = 0;
      }
      connectionState = state;
      updateConnectionIndicator(state);
    },
    () => ({
      type: 'resume',
      initialized: synchronizedBaseline,
      states: synchronizedBaseline ? [...synchronizedStates.values()] : initialResumeStates(),
    }),
  );

  socket.start();
  initLiveCommentSubmit(socket);
  initPendingOperationObserver();

  document.addEventListener(issueLiveRefreshEvent, () => socket.refresh());

  let visibilitySuspendTimer: number | null = null;
  const cancelVisibilitySuspend = () => {
    if (visibilitySuspendTimer === null) return;
    window.clearTimeout(visibilitySuspendTimer);
    visibilitySuspendTimer = null;
  };
  document.addEventListener('visibilitychange', () => {
    if (document.visibilityState === 'hidden') {
      cancelVisibilitySuspend();
      // Browser chrome and mobile task switching can briefly hide the page.
      // Keep the socket alive for a grace period to avoid reconnect churn.
      visibilitySuspendTimer = window.setTimeout(() => {
        visibilitySuspendTimer = null;
        socket.suspend();
      }, hiddenSocketSuspendDelay);
    } else {
      cancelVisibilitySuspend();
      socket.resume();
      flushPendingOperations();
    }
  });
  window.addEventListener('pagehide', () => {
    cancelVisibilitySuspend();
    socket.suspend();
  });
  window.addEventListener('pageshow', () => socket.resume());
}
