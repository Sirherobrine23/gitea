export {};

type IssueLiveState = {
  key: string,
  beforeKey?: string,
  hash?: string,
  contentVersion?: string,
  reactionState?: string,
  attachmentState?: string,
};

type IssueLiveClientMessage = {
  type: 'resume',
  initialized: boolean,
  states: IssueLiveState[],
};

type WorkerCommand = {
  type: 'start' | 'refresh' | 'status' | 'close' | 'resume',
  url?: string,
  resume?: IssueLiveClientMessage,
};

type ConnectionState = 'connecting' | 'open' | 'closed' | 'error';

function normalizeResumeMessage(message?: IssueLiveClientMessage): IssueLiveClientMessage {
  if (message?.type !== 'resume' || !Array.isArray(message.states)) {
    return {type: 'resume', initialized: false, states: []};
  }
  return {type: 'resume', initialized: message.initialized, states: message.states};
}

class SocketSource {
  url: string;
  socket: WebSocket | null = null;
  clients = new Set<MessagePort>();
  reconnectTimer: number | null = null;
  reconnectDelay = 1000;
  closed = false;
  pendingRefresh = false;
  state: ConnectionState = 'closed';
  resumeMessage: IssueLiveClientMessage;

  constructor(url: string, resumeMessage?: IssueLiveClientMessage) {
    this.url = url;
    this.resumeMessage = normalizeResumeMessage(resumeMessage);
    this.connect();
  }

  register(port: MessagePort, resumeMessage?: IssueLiveClientMessage) {
    this.updateResumeMessage(resumeMessage);
    this.clients.add(port);
    this.sendStatus(port);
  }

  deregister(port: MessagePort) {
    this.clients.delete(port);
    return this.clients.size;
  }

  updateResumeMessage(resumeMessage?: IssueLiveClientMessage) {
    if (resumeMessage) this.resumeMessage = normalizeResumeMessage(resumeMessage);
  }

  notify(message: Record<string, unknown>) {
    for (const client of this.clients) {
      client.postMessage(message);
    }
  }

  setState(state: ConnectionState) {
    this.state = state;
    this.notify({type: 'connection-status', state});
  }

  sendStatus(port: MessagePort) {
    port.postMessage({type: 'connection-status', state: this.state});
  }

  connect() {
    if (this.closed || this.socket?.readyState === WebSocket.CONNECTING || this.socket?.readyState === WebSocket.OPEN) return;

    this.setState('connecting');
    const socket = new WebSocket(this.url);
    this.socket = socket;

    socket.addEventListener('open', () => {
      if (this.socket !== socket) return;
      try {
        socket.send(JSON.stringify(this.resumeMessage));
      } catch (error) {
        this.notify({type: 'worker-error', message: String(error)});
        socket.close(1011, 'Unable to resume');
        return;
      }
      this.reconnectDelay = 1000;
      this.setState('open');
      if (this.pendingRefresh) {
        this.pendingRefresh = false;
        this.sendRefresh();
      }
    });

    socket.addEventListener('message', (event) => {
      let message: Record<string, unknown>;
      try {
        message = JSON.parse(String(event.data));
      } catch {
        message = {type: 'message', data: event.data};
      }
      this.notify(message);
    });

    socket.addEventListener('error', () => {
      if (this.socket === socket) this.setState('error');
    });

    socket.addEventListener('close', () => {
      if (this.socket !== socket) return;
      this.socket = null;
      this.setState('closed');
      this.scheduleReconnect();
    });
  }

  scheduleReconnect() {
    if (this.closed || !this.clients.size || this.reconnectTimer !== null) return;
    const delay = this.reconnectDelay;
    this.reconnectDelay = Math.min(this.reconnectDelay * 2, 30000);
    this.reconnectTimer = self.setTimeout(() => {
      this.reconnectTimer = null;
      this.connect();
    }, delay);
  }

  sendRefresh() {
    if (this.socket?.readyState === WebSocket.OPEN) {
      this.socket.send(JSON.stringify({type: 'refresh'}));
    } else {
      this.pendingRefresh = true;
      this.connect();
    }
  }

  close() {
    this.closed = true;
    if (this.reconnectTimer !== null) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    this.socket?.close(1000, 'No active tabs');
    this.socket = null;
    this.setState('closed');
  }
}

const sourcesByUrl = new Map<string, SocketSource>();
const sourcesByPort = new Map<MessagePort, SocketSource>();

function detachPort(port: MessagePort) {
  const source = sourcesByPort.get(port);
  if (!source) return;

  sourcesByPort.delete(port);
  if (source.deregister(port) === 0) {
    source.close();
    sourcesByUrl.delete(source.url);
  }
}

(self as unknown as SharedWorkerGlobalScope).addEventListener('connect', (event: MessageEvent) => {
  for (const port of event.ports) {
    port.addEventListener('message', (messageEvent: MessageEvent<WorkerCommand>) => {
      const command = messageEvent.data;
      if (!command?.type) return;

      if (command.type === 'start') {
        if (!command.url) {
          port.postMessage({type: 'worker-error', message: 'Missing WebSocket URL'});
          return;
        }

        const previous = sourcesByPort.get(port);
        if (previous?.url === command.url) {
          previous.updateResumeMessage(command.resume);
          previous.sendStatus(port);
          return;
        }
        if (previous) detachPort(port);

        let source = sourcesByUrl.get(command.url);
        if (!source) {
          source = new SocketSource(command.url, command.resume);
          sourcesByUrl.set(command.url, source);
        }
        source.register(port, command.resume);
        sourcesByPort.set(port, source);
      } else if (command.type === 'resume') {
        sourcesByPort.get(port)?.updateResumeMessage(command.resume);
      } else if (command.type === 'refresh') {
        sourcesByPort.get(port)?.sendRefresh();
      } else if (command.type === 'status') {
        const source = sourcesByPort.get(port);
        if (source) {
          source.sendStatus(port);
        } else {
          port.postMessage({type: 'connection-status', state: 'closed'});
        }
      } else if (command.type === 'close') {
        detachPort(port);
        port.close();
      }
    });
    port.start();
  }
});
