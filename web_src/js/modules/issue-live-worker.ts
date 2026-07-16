export type IssueLiveOperation = {
  action: 'upsert' | 'delete',
  key: string,
  beforeKey?: string,
  html?: string,
};

export type IssueLiveWorkerEvent = {
  type: 'connection-status' | 'timeline' | 'worker-error',
  state?: 'connecting' | 'open' | 'closed' | 'error',
  sequence?: number,
  operations?: IssueLiveOperation[],
  message?: string,
};

export class IssueLiveSharedWorker {
  sharedWorker: SharedWorker;

  constructor(url: string) {
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
    this.sharedWorker.port.postMessage({type: 'start', url});
  }

  addMessageEventListener(listener: (event: MessageEvent<IssueLiveWorkerEvent>) => void) {
    this.sharedWorker.port.addEventListener('message', listener);
  }

  startPort() {
    this.sharedWorker.port.start();
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
