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
  type: 'start' | 'refresh' | 'status' | 'close' | 'resume' | 'check',
  url?: string,
  resume?: IssueLiveClientMessage,
};

type ConnectionState = 'connecting' | 'open' | 'closed' | 'error';

const reconnectInitialDelay = 1000;
const reconnectMaxDelay = 10000;
const connectTimeout = 10000;
const heartbeatInterval = 15000;
const heartbeatTimeout = 10000;

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
  reconnectDelay = reconnectInitialDelay;
  connectTimer: number | null = null;
  heartbeatTimer: number | null = null;
  heartbeatTimeoutTimer: number | null = null;
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
    this.checkConnection();
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

  clearReconnectTimer() {
    if (this.reconnectTimer === null) return;
    clearTimeout(this.reconnectTimer);
    this.reconnectTimer = null;
  }

  clearConnectTimer() {
    if (this.connectTimer === null) return;
    clearTimeout(this.connectTimer);
    this.connectTimer = null;
  }

  clearHeartbeatTimeout() {
    if (this.heartbeatTimeoutTimer === null) return;
    clearTimeout(this.heartbeatTimeoutTimer);
    this.heartbeatTimeoutTimer = null;
  }

  stopHeartbeat() {
    if (this.heartbeatTimer !== null) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
    this.clearHeartbeatTimeout();
  }

  resetSocket(socket: WebSocket, state: ConnectionState) {
    if (this.socket !== socket) return;
    this.socket = null;
    this.clearConnectTimer();
    this.stopHeartbeat();
    this.setState(state);
    try {
      socket.close();
    } catch {
      // The socket is already unusable.
    }
    this.scheduleReconnect();
  }

  connect() {
    if (this.closed || this.socket?.readyState === WebSocket.CONNECTING || this.socket?.readyState === WebSocket.OPEN) return;

    this.clearReconnectTimer();
    this.setState('connecting');
    const socket = new WebSocket(this.url);
    this.socket = socket;
    this.connectTimer = self.setTimeout(() => {
      this.resetSocket(socket, 'error');
    }, connectTimeout);

    socket.addEventListener('open', () => {
      if (this.socket !== socket) return;
      this.clearConnectTimer();
      try {
        socket.send(JSON.stringify(this.resumeMessage));
      } catch (error) {
        this.notify({type: 'worker-error', message: String(error)});
        this.resetSocket(socket, 'error');
        return;
      }
      this.reconnectDelay = reconnectInitialDelay;
      this.setState('open');
      this.startHeartbeat();
      if (this.pendingRefresh) {
        this.pendingRefresh = false;
        this.sendRefresh();
      }
    });

    socket.addEventListener('message', (event) => {
      if (this.socket !== socket) return;
      let message: Record<string, unknown>;
      try {
        message = JSON.parse(String(event.data));
      } catch {
        message = {type: 'message', data: event.data};
      }
      if (message.type === 'pong') {
        this.clearHeartbeatTimeout();
        return;
      }
      this.notify(message);
    });

    socket.addEventListener('error', () => {
      this.resetSocket(socket, 'error');
    });

    socket.addEventListener('close', () => {
      if (this.socket !== socket) return;
      this.socket = null;
      this.clearConnectTimer();
      this.stopHeartbeat();
      this.setState('closed');
      this.scheduleReconnect();
    });
  }

  scheduleReconnect() {
    if (this.closed || !this.clients.size || this.reconnectTimer !== null) return;
    const delay = this.reconnectDelay;
    this.reconnectDelay = Math.min(this.reconnectDelay * 2, reconnectMaxDelay);
    this.reconnectTimer = self.setTimeout(() => {
      this.reconnectTimer = null;
      this.connect();
    }, delay);
  }

  startHeartbeat() {
    this.stopHeartbeat();
    this.heartbeatTimer = self.setInterval(() => this.sendHeartbeat(), heartbeatInterval);
  }

  sendHeartbeat() {
    const socket = this.socket;
    if (socket?.readyState !== WebSocket.OPEN || this.heartbeatTimeoutTimer !== null) return;
    try {
      socket.send(JSON.stringify({type: 'ping'}));
    } catch {
      this.resetSocket(socket, 'error');
      return;
    }
    this.heartbeatTimeoutTimer = self.setTimeout(() => {
      this.resetSocket(socket, 'error');
    }, heartbeatTimeout);
  }

  sendRefresh() {
    const socket = this.socket;
    if (socket?.readyState === WebSocket.OPEN) {
      try {
        socket.send(JSON.stringify({type: 'refresh'}));
      } catch {
        this.pendingRefresh = true;
        this.resetSocket(socket, 'error');
      }
    } else {
      this.pendingRefresh = true;
      this.connect();
    }
  }

  checkConnection() {
    if (this.closed || !this.clients.size) return;
    if (this.reconnectTimer !== null) {
      this.clearReconnectTimer();
      this.connect();
      return;
    }
    if (!this.socket || this.socket.readyState === WebSocket.CLOSED || this.socket.readyState === WebSocket.CLOSING) {
      this.connect();
      return;
    }
    if (this.socket.readyState === WebSocket.OPEN) this.sendHeartbeat();
  }

  close() {
    this.closed = true;
    this.clearReconnectTimer();
    this.clearConnectTimer();
    this.stopHeartbeat();
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
          previous.checkConnection();
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
      } else if (command.type === 'check') {
        sourcesByPort.get(port)?.checkConnection();
      } else if (command.type === 'close') {
        detachPort(port);
        port.close();
      }
    });
    port.start();
  }
});
