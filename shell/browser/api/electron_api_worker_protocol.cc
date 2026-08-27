// Copyright (c) 2026 Anthropic, PBC.
// Use of this source code is governed by the MIT license that can be
// found in the LICENSE file.

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base/functional/bind.h"
#include "base/memory/raw_ptr.h"
#include "content/public/browser/browser_task_traits.h"
#include "content/public/browser/browser_thread.h"
#include "gin/converter.h"
#include "gin/dictionary.h"
#include "net/base/net_errors.h"
#include "shell/browser/electron_browser_context.h"
#include "shell/browser/net/worker_protocol.h"
#include "shell/browser/protocol_registry.h"
#include "shell/common/gin_converters/gurl_converter.h"
#include "shell/common/gin_converters/std_converter.h"
#include "shell/common/gin_helper/dictionary.h"
#include "shell/common/node_includes.h"

// protocol.handle() for Node.js worker threads: lib/worker_thread/api/protocol
// drives this binding; requests arrive from the IO thread through a
// WorkerProtocolEndpoint and never touch the browser's main thread.

namespace {

using electron::WorkerProtocolEndpoint;

// Per worker environment. Owns the JS callbacks and the endpoint; torn down by
// the environment's cleanup hook.
class WorkerProtocolBinding : public WorkerProtocolEndpoint::Delegate {
 public:
  WorkerProtocolBinding(v8::Isolate* isolate, v8::Local<v8::Context> context)
      : isolate_(isolate), context_(isolate, context) {
    endpoint_ = base::MakeRefCounted<WorkerProtocolEndpoint>(
        node::GetCurrentEventLoop(isolate), this);
    node::AddEnvironmentCleanupHook(isolate, &WorkerProtocolBinding::Cleanup,
                                    this);
  }

  static void Cleanup(void* arg) {
    auto* self = static_cast<WorkerProtocolBinding*>(arg);
    for (const auto& [scheme, partition] : self->schemes_)
      self->PostUnregister(partition, scheme);
    // Registrations still in flight land before this on the UI thread.
    for (const auto& [token, pending] : self->pending_)
      self->PostUnregister(pending.second, pending.first);
    self->endpoint_->Close();
    delete self;
  }

  // JS: setCallbacks(onRequest, onCancel, onWritten)
  void SetCallbacks(v8::Local<v8::Function> on_request,
                    v8::Local<v8::Function> on_cancel,
                    v8::Local<v8::Function> on_written) {
    on_request_.Reset(isolate_, on_request);
    on_cancel_.Reset(isolate_, on_cancel);
    on_written_.Reset(isolate_, on_written);
  }

  // JS: handle(partition, scheme) -> Promise<boolean> resolved on this thread
  // once the UI thread has (or has not) registered the scheme.
  v8::Local<v8::Promise> Handle(const std::string& partition,
                                const std::string& scheme) {
    v8::Local<v8::Context> context = context_.Get(isolate_);
    auto resolver = v8::Promise::Resolver::New(context).ToLocalChecked();
    const uint64_t token = ++next_token_;
    resolvers_[token].Reset(isolate_, resolver);
    pending_[token] = {scheme, partition};
    UpdateKeepAlive();
    content::GetUIThreadTaskRunner({})->PostTask(
        FROM_HERE,
        base::BindOnce(
            [](scoped_refptr<WorkerProtocolEndpoint> endpoint,
               std::string partition, std::string scheme, uint64_t token,
               WorkerProtocolBinding* self) {
              // Only the default session for now; `partition` is plumbed for
              // a session option later.
              auto* context =
                  electron::ElectronBrowserContext::From(partition, false);
              bool ok = context->protocol_registry()->RegisterWorkerProtocol(
                  scheme, endpoint);
              endpoint->PostToWorker(base::BindOnce(
                  &WorkerProtocolBinding::OnHandled, base::Unretained(self),
                  token, scheme, partition, ok));
            },
            endpoint_, partition, scheme, token, this));
    return resolver->GetPromise();
  }

  void Unhandle(const std::string& scheme) {
    if (auto it = schemes_.find(scheme); it != schemes_.end()) {
      PostUnregister(it->second, scheme);
      schemes_.erase(it);
    } else {
      // Still registering: OnHandled() releases it when the reply arrives.
      std::erase_if(pending_, [&](const auto& entry) {
        return entry.second.first == scheme;
      });
    }
    UpdateKeepAlive();
  }

  void Respond(double id,
               int status,
               const std::string& status_text,
               const std::vector<std::vector<std::string>>& headers,
               bool has_body) {
    std::vector<std::pair<std::string, std::string>> pairs;
    pairs.reserve(headers.size());
    for (const auto& header : headers) {
      if (header.size() == 2)
        pairs.emplace_back(header[0], header[1]);
    }
    endpoint_->Respond(static_cast<uint64_t>(id), status, status_text,
                       std::move(pairs), has_body);
  }

  void Write(double id, v8::Local<v8::Value> chunk) {
    if (!chunk->IsArrayBufferView())
      return;
    auto view = chunk.As<v8::ArrayBufferView>();
    std::string bytes(view->ByteLength(), '\0');
    view->CopyContents(bytes.data(), bytes.size());
    endpoint_->Write(static_cast<uint64_t>(id), std::move(bytes));
  }

  void Finish(double id, int net_error) {
    endpoint_->Finish(static_cast<uint64_t>(id), net_error);
  }

 private:
  ~WorkerProtocolBinding() override = default;

  void PostUnregister(const std::string& partition, const std::string& scheme) {
    content::GetUIThreadTaskRunner({})->PostTask(
        FROM_HERE, base::BindOnce(
                       [](scoped_refptr<WorkerProtocolEndpoint> endpoint,
                          std::string partition, std::string scheme) {
                         electron::ElectronBrowserContext::From(partition,
                                                                false)
                             ->protocol_registry()
                             ->UnregisterWorkerProtocol(scheme, endpoint.get());
                       },
                       endpoint_, partition, scheme));
  }

  void OnHandled(uint64_t token,
                 std::string scheme,
                 std::string partition,
                 bool ok) {
    v8::HandleScope handle_scope(isolate_);
    v8::Local<v8::Context> context = context_.Get(isolate_);
    v8::Context::Scope context_scope(context);
    const bool wanted = pending_.erase(token) > 0;
    if (ok && wanted)
      schemes_[scheme] = partition;
    else if (ok)
      PostUnregister(partition, scheme);
    ok = ok && wanted;
    auto it = resolvers_.find(token);
    if (it == resolvers_.end())
      return;
    v8::Local<v8::Promise::Resolver> resolver = it->second.Get(isolate_);
    resolvers_.erase(it);
    UpdateKeepAlive();
    resolver->Resolve(context, v8::Boolean::New(isolate_, ok)).ToChecked();
  }

  void UpdateKeepAlive() {
    endpoint_->SetKeepAlive(!schemes_.empty() || !resolvers_.empty());
  }

  // WorkerProtocolEndpoint::Delegate:
  void OnWorkerProtocolEvents(
      std::vector<WorkerProtocolEndpoint::Event> events) override {
    v8::HandleScope handle_scope(isolate_);
    v8::Local<v8::Context> context = context_.Get(isolate_);
    v8::Context::Scope context_scope(context);
    node::Environment* env = node::GetCurrentEnvironment(context);
    for (auto& event : events) {
      using Kind = WorkerProtocolEndpoint::Event::Kind;
      v8::Local<v8::Function> fn;
      std::vector<v8::Local<v8::Value>> args;
      args.push_back(v8::Number::New(isolate_, static_cast<double>(event.id)));
      if (event.kind == Kind::kRequest) {
        fn = on_request_.Get(isolate_);
        const auto& r = *event.request;
        gin_helper::Dictionary dict = gin::Dictionary::CreateEmpty(isolate_);
        dict.Set("scheme", r.scheme);
        dict.Set("url", r.url.spec());
        dict.Set("method", r.method);
        dict.Set("referrer", r.referrer);
        std::vector<std::vector<std::string>> headers;
        headers.reserve(r.headers.size());
        for (const auto& [name, value] : r.headers)
          headers.push_back({name, value});
        dict.Set("headers", headers);
        v8::Local<v8::Object> body;
        if (r.body &&
            node::Buffer::Copy(isolate_, r.body->data(), r.body->size())
                .ToLocal(&body)) {
          dict.Set("body", body);
        }
        args.push_back(gin::ConvertToV8(isolate_, dict));
      } else if (event.kind == Kind::kCancel) {
        fn = on_cancel_.Get(isolate_);
      } else if (event.kind == Kind::kCallback) {
        std::move(event.callback).Run();
        continue;
      } else {
        fn = on_written_.Get(isolate_);
        args.push_back(v8::Number::New(
            isolate_, static_cast<double>(event.bytes_written)));
      }
      if (fn.IsEmpty())
        continue;
      node::CallbackScope callback_scope(env, v8::Object::New(isolate_),
                                         {0, 0});
      std::ignore =
          fn->Call(context, v8::Undefined(isolate_), args.size(), args.data());
    }
  }

  const raw_ptr<v8::Isolate> isolate_;
  v8::Global<v8::Context> context_;
  scoped_refptr<WorkerProtocolEndpoint> endpoint_;
  v8::Global<v8::Function> on_request_, on_cancel_, on_written_;
  std::map<std::string, std::string> schemes_;  // scheme -> partition
  uint64_t next_token_ = 0;
  std::map<uint64_t, v8::Global<v8::Promise::Resolver>> resolvers_;
  // token -> (scheme, partition) of registrations awaiting the UI thread.
  std::map<uint64_t, std::pair<std::string, std::string>> pending_;
};

void Initialize(v8::Local<v8::Object> exports,
                v8::Local<v8::Value> unused,
                v8::Local<v8::Context> context,
                void* priv) {
  v8::Isolate* const isolate = v8::Isolate::GetCurrent();
  // Leaked into the environment's cleanup hook.
  auto* binding = new WorkerProtocolBinding(isolate, context);
  gin_helper::Dictionary dict{isolate, exports};
  dict.SetMethod("setCallbacks",
                 base::BindRepeating(&WorkerProtocolBinding::SetCallbacks,
                                     base::Unretained(binding)));
  dict.SetMethod("handle", base::BindRepeating(&WorkerProtocolBinding::Handle,
                                               base::Unretained(binding)));
  dict.SetMethod("unhandle",
                 base::BindRepeating(&WorkerProtocolBinding::Unhandle,
                                     base::Unretained(binding)));
  dict.SetMethod("respond", base::BindRepeating(&WorkerProtocolBinding::Respond,
                                                base::Unretained(binding)));
  dict.SetMethod("write", base::BindRepeating(&WorkerProtocolBinding::Write,
                                              base::Unretained(binding)));
  dict.SetMethod("finish", base::BindRepeating(&WorkerProtocolBinding::Finish,
                                               base::Unretained(binding)));
}

}  // namespace

NODE_LINKED_BINDING_CONTEXT_AWARE(electron_worker_protocol, Initialize)
