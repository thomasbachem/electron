// Copyright (c) 2026 Anthropic, PBC.
// Use of this source code is governed by the MIT license that can be
// found in the LICENSE file.

#ifndef ELECTRON_SHELL_BROWSER_NET_WORKER_PROTOCOL_H_
#define ELECTRON_SHELL_BROWSER_NET_WORKER_PROTOCOL_H_

#include <stdint.h>

#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "base/functional/callback.h"
#include "base/memory/raw_ptr.h"
#include "base/memory/ref_counted.h"
#include "base/memory/weak_ptr.h"
#include "base/synchronization/lock.h"
#include "mojo/public/cpp/bindings/pending_receiver.h"
#include "mojo/public/cpp/bindings/pending_remote.h"
#include "services/network/public/mojom/url_loader_factory.mojom-forward.h"
#include "url/gurl.h"
#include "uv.h"  // NOLINT(build/include_directory)

namespace electron {

class WorkerProtocolLoader;

// Connects protocol handlers registered from a Node.js worker thread with the
// URLLoaderFactories that serve their schemes on the IO thread. The IO side
// queues work and wakes the worker's uv loop; the worker side (the
// electron_worker_protocol binding) drains the queue, runs the JS handler and
// posts the response back to the IO thread. Lives until both sides let go.
class WorkerProtocolEndpoint
    : public base::RefCountedThreadSafe<WorkerProtocolEndpoint> {
 public:
  struct Request {
    Request();
    Request(Request&&);
    ~Request();
    uint64_t id = 0;
    std::string scheme;
    GURL url;
    std::string method;
    std::string referrer;
    std::vector<std::pair<std::string, std::string>> headers;
    std::optional<std::string> body;
  };
  // What the worker thread finds in its inbox.
  struct Event {
    enum class Kind { kRequest, kCancel, kWritten, kCallback };
    Event(Kind kind, uint64_t id, std::unique_ptr<Request> request = nullptr);
    explicit Event(base::OnceClosure callback);
    Event(Event&&);
    ~Event();
    Kind kind;
    uint64_t id = 0;
    std::unique_ptr<Request> request;  // kRequest
    uint64_t bytes_written = 0;        // kWritten: total so far
    base::OnceClosure callback;        // kCallback
  };

  class Delegate {
   public:
    // Called on the worker thread with everything queued since the last call.
    virtual void OnWorkerProtocolEvents(std::vector<Event> events) = 0;

   protected:
    virtual ~Delegate() = default;
  };

  // `loop` is the worker's; `delegate` is called on that thread until Close().
  WorkerProtocolEndpoint(uv_loop_t* loop, Delegate* delegate);

  // Worker thread: stop delivering events; pending and future requests fail.
  void Close();
  // Worker thread: whether this endpoint keeps the worker's event loop alive
  // (it does while the worker handles a scheme, like a listening server).
  void SetKeepAlive(bool keep_alive);

  // IO thread -> worker.
  void QueueRequest(std::unique_ptr<Request> request,
                    base::WeakPtr<WorkerProtocolLoader> loader);
  void QueueCancel(uint64_t id);
  void QueueWritten(uint64_t id, uint64_t bytes_written);
  // Any thread -> worker: run `callback` on the worker thread.
  void PostToWorker(base::OnceClosure callback);

  // Worker thread -> IO: the pieces of a response for request `id`.
  void Respond(uint64_t id,
               int status,
               std::string status_text,
               std::vector<std::pair<std::string, std::string>> headers,
               bool has_body);
  void Write(uint64_t id, std::string chunk);
  // The handler is done with `id` (net::OK or an error).
  void Finish(uint64_t id, int net_error);

 private:
  friend class base::RefCountedThreadSafe<WorkerProtocolEndpoint>;
  ~WorkerProtocolEndpoint();

  static void OnAsync(uv_async_t* handle);
  void Post(Event event);

  mutable base::Lock lock_;
  raw_ptr<uv_async_t> async_ GUARDED_BY(lock_);  // null once closed
  raw_ptr<Delegate> delegate_ GUARDED_BY(lock_);
  std::vector<Event> inbox_ GUARDED_BY(lock_);
  // IO thread only.
  std::map<uint64_t, base::WeakPtr<WorkerProtocolLoader>> loaders_;
};

// Creates, on the IO thread, a URLLoaderFactory that serves requests through
// `endpoint`, and returns the other end.
mojo::PendingRemote<network::mojom::URLLoaderFactory>
CreateWorkerProtocolURLLoaderFactory(
    scoped_refptr<WorkerProtocolEndpoint> endpoint);

}  // namespace electron

#endif  // ELECTRON_SHELL_BROWSER_NET_WORKER_PROTOCOL_H_
