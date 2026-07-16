export type IssueLiveState = {
  key: string,
  beforeKey?: string,
  hash?: string,
  contentVersion?: string,
  reactionState?: string,
  attachmentState?: string,
};

export type IssueLiveClientMessage = {
  type: 'resume',
  initialized: boolean,
  states: IssueLiveState[],
};

export type IssueLiveOperation = {
  action: 'upsert' | 'delete',
  key: string,
  beforeKey?: string,
  html?: string,
  hash?: string,
};

export type IssueLiveWorkerEvent = {
  type: 'connection-status' | 'timeline' | 'baseline' | 'worker-error',
  state?: 'connecting' | 'open' | 'closed' | 'error',
  sequence?: number,
  operations?: IssueLiveOperation[],
  states?: IssueLiveState[],
  message?: string,
};

export class IssueLiveSharedWorker {
  sharedWorker: SharedWorker;

  constructor(url: string, resume: IssueLiveClientMessage) {
    this.sharedWorker = new SharedWorker(
      new URL('../websocket.sharedworker.ts', import.meta.url),
      {name: 'issue-live-websocket', type: 'module'},
    );

    this.sharedWorker.addEventListener('error', (event) => {
      console.error('issue live worker error', event);
    });
    this.sharedWorker.port.addEventListener('messageerror', () => {
      console.error('unable to deserialize issue live worker message');
    });
    this.sharedWorker.port.postMessage({type: 'start', url, resume});
  }

  addMessageEventListener(listener: (event: MessageEvent<IssueLiveWorkerEvent>) => void) {
    this.sharedWorker.port.addEventListener('message', listener);
  }

  startPort() {
    this.sharedWorker.port.start();
  }

  updateResume(resume: IssueLiveClientMessage) {
    this.sharedWorker.port.postMessage({type: 'resume', resume});
  }

  refresh() {
    this.sharedWorker.port.postMessage({type: 'refresh'});
  }

  status() {
    this.sharedWorker.port.postMessage({type: 'status'});
  }

  close() {
    this.sharedWorker.port.postMessage({type: 'close'});
    this.sharedWorker.port.close();
  }
}
