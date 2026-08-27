// Copyright (c) 2026 Anthropic, PBC.
// Use of this source code is governed by the MIT license that can be
// found in the LICENSE file.

#include "shell/browser/net/worker_protocol.h"

#include <algorithm>
#include <deque>

#include "base/functional/bind.h"
#include "base/functional/callback_helpers.h"
#include "base/memory/self_deleting.h"
#include "base/strings/string_view_util.h"
#include "base/strings/stringprintf.h"
#include "content/public/browser/browser_task_traits.h"
#include "content/public/browser/browser_thread.h"
#include "mojo/public/cpp/bindings/receiver.h"
#include "mojo/public/cpp/bindings/remote.h"
#include "mojo/public/cpp/system/data_pipe_drainer.h"
#include "mojo/public/cpp/system/data_pipe_producer.h"
#include "mojo/public/cpp/system/string_data_source.h"
#include "net/base/net_errors.h"
#include "net/http/http_response_headers.h"
#include "net/http/http_status_code.h"
#include "net/http/http_util.h"
#include "net/url_request/redirect_info.h"
#include "net/url_request/redirect_util.h"
#include "net/url_request/referrer_policy.h"
#include "services/network/public/cpp/cors/cors.h"
#include "services/network/public/cpp/cors/cors_error_status.h"
#include "services/network/public/cpp/http_request_headers_update_params.h"
#include "services/network/public/cpp/resource_request.h"
#include "services/network/public/cpp/resource_request_body.h"
#include "services/network/public/cpp/self_deleting_url_loader_factory.h"
#include "services/network/public/cpp/url_loader_completion_status.h"
#include "services/network/public/mojom/chunked_data_pipe_getter.mojom.h"
#include "services/network/public/mojom/data_pipe_getter.mojom.h"
#include "services/network/public/mojom/url_loader.mojom.h"
#include "services/network/public/mojom/url_loader_factory.mojom.h"
#include "services/network/public/mojom/url_response_head.mojom.h"
#include "url/url_util.h"

namespace electron {

namespace {

// Request bodies are read whole before the handler runs; larger ones fail.
constexpr size_t kMaxRequestBodyBytes = 64 * 1024 * 1024;

scoped_refptr<base::SequencedTaskRunner> IOThread() {
  return content::GetIOThreadTaskRunner({});
}

}  // namespace

namespace {

uint64_t NextRequestId() {
  static uint64_t next = 0;
  return ++next;
}

std::unique_ptr<WorkerProtocolEndpoint::Request> MakeRequest(
    uint64_t id,
    const network::ResourceRequest& request,
    const std::optional<std::string>& body) {
  auto message = std::make_unique<WorkerProtocolEndpoint::Request>();
  message->id = id;
  message->scheme = request.url.scheme();
  message->url = request.url;
  message->method = request.method;
  message->referrer = request.referrer.spec();
  for (const auto& header : request.headers.GetHeaderVector())
    message->headers.emplace_back(header.key, header.value);
  message->body = body;
  return message;
}

}  // namespace

// One request being answered by a worker: owns the renderer's URLLoader pipe
// and client on the IO thread and feeds them from what the worker sends.
class WorkerProtocolLoader : public network::mojom::URLLoader {
 public:
  WorkerProtocolLoader(
      scoped_refptr<WorkerProtocolEndpoint> endpoint,
      uint64_t id,
      const network::ResourceRequest& request,
      mojo::PendingReceiver<network::mojom::URLLoader> loader,
      mojo::PendingRemote<network::mojom::URLLoaderClient> client)
      : endpoint_(std::move(endpoint)),
        id_(id),
        request_(request),
        loader_(this, std::move(loader)),
        client_(std::move(client)) {
    loader_.set_disconnect_handler(
        base::BindOnce(&WorkerProtocolLoader::Cancel, base::Unretained(this)));
    client_.set_disconnect_handler(
        base::BindOnce(&WorkerProtocolLoader::Cancel, base::Unretained(this)));
  }
  ~WorkerProtocolLoader() override = default;

  base::WeakPtr<WorkerProtocolLoader> GetWeakPtr() {
    return weak_factory_.GetWeakPtr();
  }

  // Queues the request to the worker once its body (if any) has been read.
  void Start(std::optional<std::string> body) {
    body_ = std::move(body);
    endpoint_->QueueRequest(MakeRequest(id_, request_, body_), GetWeakPtr());
  }

  void OnHead(int status,
              const std::string& status_text,
              const std::vector<std::pair<std::string, std::string>>& headers,
              bool has_body) {
    if (head_sent_)
      return;
    head_sent_ = true;
    auto head = network::mojom::URLResponseHead::New();
    // Per-scheme factories bypass CorsURLLoader, so a cross-origin no-cors
    // response is tagged opaque here as ElectronURLLoaderFactory does.
    if (request_.mode == network::mojom::RequestMode::kNoCors &&
        request_.request_initiator &&
        !request_.request_initiator->IsSameOriginWith(request_.url)) {
      head->response_type = network::mojom::FetchResponseType::kOpaque;
    }
    head->headers = base::MakeRefCounted<net::HttpResponseHeaders>(
        net::HttpUtil::AssembleRawHeaders(base::StringPrintf(
            "HTTP/1.1 %d %s", status,
            status_text.empty() ? net::GetHttpReasonPhrase(
                                      static_cast<net::HttpStatusCode>(status))
                                : status_text.c_str())));
    for (const auto& [name, value] : headers) {
      if (net::HttpUtil::IsValidHeaderName(name) &&
          net::HttpUtil::IsValidHeaderValue(value)) {
        head->headers->AddHeader(name, value);
      }
    }
    head->headers->GetMimeTypeAndCharset(&head->mime_type, &head->charset);
    if (head->mime_type.empty())
      head->mime_type = "text/plain";
    if (auto length = head->headers->GetContentLength())
      head->content_length = length->InBytes();

    std::string location;
    if (net::HttpResponseHeaders::IsRedirectResponseCode(status) &&
        head->headers->IsRedirect(&location)) {
      redirect_info_ = net::RedirectInfo::ComputeRedirectInfo(
          request_.method, request_.url, request_.site_for_cookies,
          net::RedirectInfo::FirstPartyURLPolicy::UPDATE_URL_ON_REDIRECT,
          request_.referrer_policy, request_.referrer.GetAsReferrer().spec(),
          request_.request_initiator, status, request_.url.Resolve(location),
          net::RedirectUtil::GetReferrerPolicyHeader(head->headers.get()),
          /*insecure_scheme_was_upgraded=*/false);
      // Navigations and net.fetch start over with a new loader; a renderer
      // subresource request calls FollowRedirect() on this one.
      client_->OnReceiveRedirect(*redirect_info_, std::move(head));
      return;
    }

    mojo::ScopedDataPipeProducerHandle producer;
    mojo::ScopedDataPipeConsumerHandle consumer;
    if (mojo::CreateDataPipe(nullptr, producer, consumer) != MOJO_RESULT_OK) {
      Complete(net::ERR_INSUFFICIENT_RESOURCES);
      return;
    }
    producer_ = std::make_unique<mojo::DataPipeProducer>(std::move(producer));
    client_->OnReceiveResponse(std::move(head), std::move(consumer),
                               std::nullopt);
  }

  void OnData(std::string chunk) {
    if (!producer_ || finished_)
      return;
    queue_.push_back(std::move(chunk));
    PumpWrites();
  }

  void OnFinish(int net_error) {
    if (redirect_info_) {
      if (net_error != net::OK)
        Complete(net_error);
      return;  // Otherwise wait for FollowRedirect() or disconnect.
    }
    finished_ = true;
    result_ = !head_sent_ && net_error == net::OK ? net::ERR_FAILED : net_error;
    if (!writing_ && queue_.empty())
      Complete(result_);
  }

  // network::mojom::URLLoader:
  void FollowRedirect(
      network::HttpRequestHeadersUpdateParams headers_update_params,
      const std::optional<GURL>& new_url) override {
    if (!redirect_info_)
      return;
    const net::RedirectInfo info = *std::exchange(redirect_info_, std::nullopt);
    const bool keeps_body = info.new_method == request_.method;
    request_.url = new_url.value_or(info.new_url);
    request_.method = info.new_method;
    request_.site_for_cookies = info.new_site_for_cookies;
    request_.referrer = GURL(info.new_referrer);
    request_.referrer_policy = info.new_referrer_policy;
    for (const auto& name : headers_update_params.removed_headers)
      request_.headers.RemoveHeader(name);
    request_.headers.MergeFrom(headers_update_params.modified_headers);
    if (!keeps_body)
      body_.reset();
    head_sent_ = false;
    id_ = NextRequestId();
    endpoint_->QueueRequest(MakeRequest(id_, request_, body_), GetWeakPtr());
  }
  void SetPriority(net::RequestPriority priority,
                   int32_t intra_priority_value) override {}

 private:
  void PumpWrites() {
    if (writing_ || queue_.empty() || !producer_)
      return;
    writing_ = true;
    in_flight_ = std::move(queue_.front());
    queue_.pop_front();
    producer_->Write(std::make_unique<mojo::StringDataSource>(
                         in_flight_, mojo::StringDataSource::AsyncWritingMode::
                                         STRING_STAYS_VALID_UNTIL_COMPLETION),
                     base::BindOnce(&WorkerProtocolLoader::DidWrite,
                                    weak_factory_.GetWeakPtr()));
  }

  void DidWrite(MojoResult result) {
    writing_ = false;
    bytes_written_ += in_flight_.size();
    in_flight_.clear();
    if (result != MOJO_RESULT_OK) {
      Complete(net::ERR_FAILED);
      return;
    }
    if (!queue_.empty()) {
      PumpWrites();
      return;
    }
    if (finished_) {
      Complete(result_);
      return;
    }
    // Lets the worker's writer know how far the renderer has read, so it can
    // pace a large body; see HIGH_WATER_MARK in lib/worker_thread/api/protocol.
    endpoint_->QueueWritten(id_, bytes_written_);
  }

  void Cancel() { Complete(net::ERR_ABORTED); }

  void Complete(int net_error) {
    if (completed_)
      return;
    completed_ = true;
    producer_.reset();
    // Lets a handler that is still producing this response stop.
    if (net_error != net::OK)
      endpoint_->QueueCancel(id_);
    if (client_.is_connected()) {
      network::URLLoaderCompletionStatus status(net_error);
      status.completion_time = base::TimeTicks::Now();
      status.encoded_data_length = base::ByteSize(bytes_written_);
      status.encoded_body_length = base::ByteSize(bytes_written_);
      status.decoded_body_length = base::ByteSize(bytes_written_);
      client_->OnComplete(status);
    }
    delete this;
  }

  const scoped_refptr<WorkerProtocolEndpoint> endpoint_;
  uint64_t id_;
  // The request being served; updated when a redirect is followed.
  network::ResourceRequest request_;
  std::optional<std::string> body_;
  std::optional<net::RedirectInfo> redirect_info_;
  mojo::Receiver<network::mojom::URLLoader> loader_;
  mojo::Remote<network::mojom::URLLoaderClient> client_;
  std::unique_ptr<mojo::DataPipeProducer> producer_;
  std::deque<std::string> queue_;
  std::string in_flight_;
  uint64_t bytes_written_ = 0;
  bool head_sent_ = false;
  bool writing_ = false;
  bool finished_ = false;
  bool completed_ = false;
  int result_ = net::OK;
  base::WeakPtrFactory<WorkerProtocolLoader> weak_factory_{this};
};

namespace {

// Collects a request body (bytes, a data pipe, or a chunked data pipe as
// net.fetch() sends) on the IO thread, then hands it over as one string.
class BodyReader : public mojo::DataPipeDrainer::Client {
 public:
  // `body` is nullopt when there is none or it exceeds kMaxRequestBodyBytes
  // (`too_large`).
  using Done =
      base::OnceCallback<void(std::optional<std::string> body, bool too_large)>;

  static void Read(scoped_refptr<network::ResourceRequestBody> body,
                   Done done) {
    if (!body) {
      std::move(done).Run(std::nullopt, false);
      return;
    }
    // Self-owned; deletes itself after running `done`.
    (new BodyReader(std::move(done)))->Start(std::move(body));
  }

 private:
  explicit BodyReader(Done done) : done_(std::move(done)) {}
  ~BodyReader() override = default;

  void Start(scoped_refptr<network::ResourceRequestBody> body) {
    elements_ = std::move(*body->elements_mutable());
    Next();
  }

  void Next() {
    if (too_large_ || index_ == elements_.size()) {
      std::move(done_).Run(too_large_
                               ? std::nullopt
                               : std::optional<std::string>(std::move(data_)),
                           too_large_);
      delete this;
      return;
    }
    network::DataElement& element = elements_[index_++];
    mojo::ScopedDataPipeProducerHandle producer;
    mojo::ScopedDataPipeConsumerHandle consumer;
    switch (element.type()) {
      case network::DataElement::Tag::kBytes:
        Append(element.As<network::DataElementBytes>().AsStringPiece());
        Next();
        return;
      case network::DataElement::Tag::kDataPipe:
        if (mojo::CreateDataPipe(nullptr, producer, consumer) !=
            MOJO_RESULT_OK) {
          Next();
          return;
        }
        data_pipe_getter_.reset();
        data_pipe_getter_.Bind(
            element.As<network::DataElementDataPipe>().ReleaseDataPipeGetter());
        data_pipe_getter_->Read(std::move(producer), base::DoNothing());
        break;
      case network::DataElement::Tag::kChunkedDataPipe:
        if (mojo::CreateDataPipe(nullptr, producer, consumer) !=
            MOJO_RESULT_OK) {
          Next();
          return;
        }
        chunked_getter_.reset();
        chunked_getter_.Bind(element.As<network::DataElementChunkedDataPipe>()
                                 .ReleaseChunkedDataPipeGetter());
        chunked_getter_->GetSize(base::DoNothing());
        chunked_getter_->StartReading(std::move(producer));
        break;
      default:
        // Renderers and net.fetch() do not produce other element types for
        // custom schemes.
        Next();
        return;
    }
    drainer_ =
        std::make_unique<mojo::DataPipeDrainer>(this, std::move(consumer));
  }

  // mojo::DataPipeDrainer::Client:
  void OnDataAvailable(base::span<const uint8_t> data) override {
    Append(base::as_string_view(data));
  }
  void Append(std::string_view bytes) {
    if (too_large_ || data_.size() + bytes.size() > kMaxRequestBodyBytes) {
      too_large_ = true;
      data_.clear();
      return;
    }
    data_.append(bytes);
  }
  void OnDataComplete() override {
    drainer_.reset();
    Next();
  }

  Done done_;
  std::vector<network::DataElement> elements_;
  size_t index_ = 0;
  bool too_large_ = false;
  std::string data_;
  std::unique_ptr<mojo::DataPipeDrainer> drainer_;
  mojo::Remote<network::mojom::DataPipeGetter> data_pipe_getter_;
  mojo::Remote<network::mojom::ChunkedDataPipeGetter> chunked_getter_;
};

class WorkerProtocolURLLoaderFactory
    : public network::SelfDeletingURLLoaderFactory {
 public:
  WorkerProtocolURLLoaderFactory(
      scoped_refptr<WorkerProtocolEndpoint> endpoint,
      mojo::PendingReceiver<network::mojom::URLLoaderFactory> receiver,
      base::SelfDeletingPassKey key)
      : network::SelfDeletingURLLoaderFactory(std::move(receiver), key),
        endpoint_(std::move(endpoint)) {}

  void CreateLoaderAndStart(
      mojo::PendingReceiver<network::mojom::URLLoader> loader,
      int32_t request_id,
      uint32_t options,
      const network::ResourceRequest& request,
      mojo::PendingRemote<network::mojom::URLLoaderClient> client,
      const net::MutableNetworkTrafficAnnotationTag& traffic_annotation)
      override {
    // Per-scheme factories bypass the network service's CorsURLLoaderFactory;
    // apply its kCorsDisabledScheme gate as ElectronURLLoaderFactory does.
    if (request.request_initiator &&
        network::cors::ShouldCheckCors(request.url, request.request_initiator,
                                       request.mode) &&
        !std::ranges::contains(url::GetCorsEnabledSchemes(),
                               request.url.GetScheme())) {
      mojo::Remote<network::mojom::URLLoaderClient>(std::move(client))
          ->OnComplete(
              network::URLLoaderCompletionStatus(network::CorsErrorStatus(
                  network::mojom::CorsError::kCorsDisabledScheme)));
      return;
    }
    // Self-owned; deletes itself when the request completes or is dropped.
    auto* in_flight =
        new WorkerProtocolLoader(endpoint_, NextRequestId(), request,
                                 std::move(loader), std::move(client));
    BodyReader::Read(request.request_body,
                     base::BindOnce(
                         [](base::WeakPtr<WorkerProtocolLoader> loader,
                            std::optional<std::string> body, bool too_large) {
                           if (!loader)
                             return;
                           if (too_large)
                             loader->OnFinish(net::ERR_FILE_TOO_BIG);
                           else
                             loader->Start(std::move(body));
                         },
                         in_flight->GetWeakPtr()));
  }

 private:
  ~WorkerProtocolURLLoaderFactory() override = default;

  const scoped_refptr<WorkerProtocolEndpoint> endpoint_;
};

}  // namespace

WorkerProtocolEndpoint::Request::Request() = default;
WorkerProtocolEndpoint::Request::Request(Request&&) = default;
WorkerProtocolEndpoint::Request::~Request() = default;

WorkerProtocolEndpoint::Event::Event(Kind kind,
                                     uint64_t id,
                                     std::unique_ptr<Request> request)
    : kind(kind), id(id), request(std::move(request)) {}
WorkerProtocolEndpoint::Event::Event(base::OnceClosure callback)
    : kind(Kind::kCallback), callback(std::move(callback)) {}
WorkerProtocolEndpoint::Event::Event(Event&&) = default;
WorkerProtocolEndpoint::Event::~Event() = default;

WorkerProtocolEndpoint::WorkerProtocolEndpoint(uv_loop_t* loop,
                                               Delegate* delegate)
    : async_(new uv_async_t), delegate_(delegate) {
  uv_async_init(loop, async_.get(), &WorkerProtocolEndpoint::OnAsync);
  async_->data = this;
  uv_unref(reinterpret_cast<uv_handle_t*>(async_.get()));
  AddRef();  // Balanced when the async handle is closed.
}

WorkerProtocolEndpoint::~WorkerProtocolEndpoint() = default;

// static
void WorkerProtocolEndpoint::OnAsync(uv_async_t* handle) {
  auto* self = static_cast<WorkerProtocolEndpoint*>(handle->data);
  std::vector<Event> events;
  Delegate* delegate;
  {
    base::AutoLock lock(self->lock_);
    events.swap(self->inbox_);
    delegate = self->delegate_.get();
  }
  if (delegate && !events.empty())
    delegate->OnWorkerProtocolEvents(std::move(events));
}

void WorkerProtocolEndpoint::SetKeepAlive(bool keep_alive) {
  base::AutoLock lock(lock_);
  if (!async_)
    return;
  if (keep_alive)
    uv_ref(reinterpret_cast<uv_handle_t*>(async_.get()));
  else
    uv_unref(reinterpret_cast<uv_handle_t*>(async_.get()));
}

void WorkerProtocolEndpoint::Close() {
  uv_async_t* async;
  std::vector<Event> dropped;
  {
    base::AutoLock lock(lock_);
    async = async_.get();
    async_ = nullptr;
    delegate_ = nullptr;
    dropped.swap(inbox_);
  }
  if (!async)
    return;
  IOThread()->PostTask(FROM_HERE,
                       base::BindOnce(
                           [](scoped_refptr<WorkerProtocolEndpoint> self) {
                             auto loaders = std::move(self->loaders_);
                             for (auto& [id, loader] : loaders) {
                               if (loader)
                                 loader->OnFinish(net::ERR_FAILED);
                             }
                           },
                           scoped_refptr<WorkerProtocolEndpoint>(this)));
  uv_close(reinterpret_cast<uv_handle_t*>(async), [](uv_handle_t* handle) {
    auto* self = static_cast<WorkerProtocolEndpoint*>(handle->data);
    delete reinterpret_cast<uv_async_t*>(handle);
    self->Release();
  });
}

void WorkerProtocolEndpoint::Post(Event event) {
  const Event::Kind kind = event.kind;
  const uint64_t id = event.id;
  {
    base::AutoLock lock(lock_);
    if (async_) {
      inbox_.push_back(std::move(event));
      uv_async_send(async_.get());
      return;
    }
  }
  if (kind == Event::Kind::kRequest)
    Finish(id, net::ERR_FAILED);
}

void WorkerProtocolEndpoint::QueueRequest(
    std::unique_ptr<Request> request,
    base::WeakPtr<WorkerProtocolLoader> loader) {
  DCHECK_CURRENTLY_ON(content::BrowserThread::IO);
  const uint64_t id = request->id;
  loaders_[id] = std::move(loader);
  Post(Event{Event::Kind::kRequest, id, std::move(request)});
}

void WorkerProtocolEndpoint::QueueCancel(uint64_t id) {
  DCHECK_CURRENTLY_ON(content::BrowserThread::IO);
  loaders_.erase(id);
  Post(Event{Event::Kind::kCancel, id});
}

void WorkerProtocolEndpoint::QueueWritten(uint64_t id, uint64_t bytes_written) {
  Event event{Event::Kind::kWritten, id};
  event.bytes_written = bytes_written;
  Post(std::move(event));
}

void WorkerProtocolEndpoint::PostToWorker(base::OnceClosure callback) {
  Post(Event(std::move(callback)));
}

void WorkerProtocolEndpoint::Respond(
    uint64_t id,
    int status,
    std::string status_text,
    std::vector<std::pair<std::string, std::string>> headers,
    bool has_body) {
  IOThread()->PostTask(
      FROM_HERE,
      base::BindOnce(
          [](scoped_refptr<WorkerProtocolEndpoint> self, uint64_t id,
             int status, std::string status_text,
             std::vector<std::pair<std::string, std::string>> headers,
             bool has_body) {
            auto it = self->loaders_.find(id);
            if (it != self->loaders_.end() && it->second)
              it->second->OnHead(status, status_text, headers, has_body);
          },
          scoped_refptr<WorkerProtocolEndpoint>(this), id, status,
          std::move(status_text), std::move(headers), has_body));
}

void WorkerProtocolEndpoint::Write(uint64_t id, std::string chunk) {
  IOThread()->PostTask(
      FROM_HERE,
      base::BindOnce(
          [](scoped_refptr<WorkerProtocolEndpoint> self, uint64_t id,
             std::string chunk) {
            auto it = self->loaders_.find(id);
            if (it != self->loaders_.end() && it->second)
              it->second->OnData(std::move(chunk));
          },
          scoped_refptr<WorkerProtocolEndpoint>(this), id, std::move(chunk)));
}

void WorkerProtocolEndpoint::Finish(uint64_t id, int net_error) {
  IOThread()->PostTask(
      FROM_HERE,
      base::BindOnce(
          [](scoped_refptr<WorkerProtocolEndpoint> self, uint64_t id,
             int net_error) {
            auto it = self->loaders_.find(id);
            if (it == self->loaders_.end())
              return;
            base::WeakPtr<WorkerProtocolLoader> loader = std::move(it->second);
            self->loaders_.erase(it);
            if (loader)
              loader->OnFinish(net_error);
          },
          scoped_refptr<WorkerProtocolEndpoint>(this), id, net_error));
}

mojo::PendingRemote<network::mojom::URLLoaderFactory>
CreateWorkerProtocolURLLoaderFactory(
    scoped_refptr<WorkerProtocolEndpoint> endpoint) {
  mojo::PendingRemote<network::mojom::URLLoaderFactory> remote;
  IOThread()->PostTask(
      FROM_HERE,
      base::BindOnce(
          [](scoped_refptr<WorkerProtocolEndpoint> endpoint,
             mojo::PendingReceiver<network::mojom::URLLoaderFactory> receiver) {
            base::MakeSelfDeleting<WorkerProtocolURLLoaderFactory>(
                std::move(endpoint), std::move(receiver));
          },  // NOLINT(clang-analyzer-cplusplus.NewDeleteLeaks)
          std::move(endpoint), remote.InitWithNewPipeAndPassReceiver()));
  return remote;
}

}  // namespace electron
