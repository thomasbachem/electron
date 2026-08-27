import type { ReadableStream, ReadableStreamDefaultReader } from 'stream/web';

// Only the browser process carries this binding; in worker threads created by a
// renderer or utility process protocol.handle() rejects.
const binding = (() => {
  try {
    return process._linkedBinding('electron_worker_protocol');
  } catch {
    return null;
  }
})();

type Handler = (request: Request) => Response | Promise<Response>;

interface IncomingRequest {
  scheme: string;
  url: string;
  method: string;
  referrer: string;
  headers: [string, string][];
  body?: Buffer;
}

interface InFlight {
  controller: AbortController;
  sent: number;
  written: number;
  resume?: () => void;
}

// net::ERR_FAILED / net::ERR_ABORTED
const ERR_FAILED = -2;
const ERR_ABORTED = -3;
// Body bytes handed to the browser ahead of what the renderer has read.
const HIGH_WATER_MARK = 512 * 1024;
const REDIRECT_STATUSES = new Set([301, 302, 303, 307, 308]);

const handlers = new Map<string, Handler>();
const inFlight = new Map<number, InFlight>();

async function serve(id: number, incoming: IncomingRequest) {
  const handler = handlers.get(incoming.scheme);
  const controller = new AbortController();
  const state: InFlight = { controller, sent: 0, written: 0 };
  inFlight.set(id, state);
  let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;
  try {
    if (!handler) throw new Error(`No handler for ${incoming.scheme}`);
    const headers = new Headers(incoming.headers);
    if (headers.get('origin') === 'null') headers.delete('origin');
    const request = new Request(incoming.url, {
      method: incoming.method,
      headers,
      referrer: incoming.referrer,
      body: incoming.body && incoming.method !== 'GET' && incoming.method !== 'HEAD' ? incoming.body : undefined,
      signal: controller.signal
    });
    const response = await handler(request);
    if (!(response instanceof Response) || response.type === 'error') {
      binding!.finish(id, ERR_FAILED);
      return;
    }
    const isRedirect = REDIRECT_STATUSES.has(response.status) && response.headers.has('location');
    const hasBody = response.body !== null && request.method !== 'HEAD' && !isRedirect;
    binding!.respond(id, response.status, response.statusText, [...response.headers], hasBody);
    if (hasBody) {
      reader = (response.body as ReadableStream<Uint8Array>).getReader();
      while (true) {
        if (controller.signal.aborted) throw controller.signal.reason;
        const { done, value } = await reader.read();
        if (done) break;
        binding!.write(id, value);
        state.sent += value.byteLength;
        if (state.sent - state.written > HIGH_WATER_MARK) {
          await new Promise<void>((resolve) => {
            state.resume = resolve;
          });
        }
      }
    }
    binding!.finish(id, 0);
  } catch {
    binding!.finish(id, controller.signal.aborted ? ERR_ABORTED : ERR_FAILED);
    reader?.cancel().catch(() => {});
  } finally {
    inFlight.delete(id);
  }
}

binding?.setCallbacks(
  (id: number, incoming: IncomingRequest) => {
    serve(id, incoming);
  },
  (id: number) => {
    const state = inFlight.get(id);
    if (!state) return;
    state.controller.abort();
    state.resume?.();
  },
  (id: number, written: number) => {
    const state = inFlight.get(id);
    if (!state) return;
    state.written = written;
    if (state.resume && state.sent - state.written <= HIGH_WATER_MARK) {
      const resume = state.resume;
      state.resume = undefined;
      resume();
    }
  }
);

export async function handle(scheme: string, handler: Handler) {
  if (!binding) throw new Error('protocol.handle() is only available in worker threads of the main process');
  if (typeof scheme !== 'string' || typeof handler !== 'function') {
    throw new TypeError('protocol.handle(scheme, handler) expects a string and a function');
  }
  if (handlers.has(scheme)) throw new Error(`Already handling ${scheme} in this worker`);
  handlers.set(scheme, handler);
  const ok = await binding.handle('', scheme);
  if (!ok) {
    handlers.delete(scheme);
    throw new Error(`Failed to register protocol: ${scheme} is already handled`);
  }
}

export function unhandle(scheme: string) {
  if (!handlers.delete(scheme)) throw new Error(`Not handling ${scheme} in this worker`);
  binding!.unhandle(scheme);
}

export function isProtocolHandled(scheme: string) {
  return handlers.has(scheme);
}
