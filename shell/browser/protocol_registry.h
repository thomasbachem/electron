// Copyright (c) 2020 Slack Technologies, Inc.
// Use of this source code is governed by the MIT license that can be
// found in the LICENSE file.

#ifndef ELECTRON_SHELL_BROWSER_PROTOCOL_REGISTRY_H_
#define ELECTRON_SHELL_BROWSER_PROTOCOL_REGISTRY_H_

#include <functional>
#include <map>
#include <string>
#include <string_view>

#include "base/memory/raw_ptr.h"
#include "base/memory/scoped_refptr.h"
#include "content/public/browser/content_browser_client.h"
#include "shell/browser/net/electron_url_loader_factory.h"

namespace content {
class BrowserContext;
}

namespace electron {

class ElectronBrowserContext;
class WorkerProtocolEndpoint;

class ProtocolRegistry {
 public:
  ~ProtocolRegistry();

  static ProtocolRegistry* FromBrowserContext(content::BrowserContext*);

  using URLLoaderFactoryType =
      content::ContentBrowserClient::URLLoaderFactoryType;

  void RegisterURLLoaderFactories(
      content::ContentBrowserClient::NonNetworkURLLoaderFactoryMap* factories,
      bool allow_file_access);

  mojo::PendingRemote<network::mojom::URLLoaderFactory>
  CreateNonNetworkNavigationURLLoaderFactory(const std::string& scheme);

  const HandlersMap& intercept_handlers() const { return intercept_handlers_; }

  bool RegisterProtocol(ProtocolType type,
                        const std::string& scheme,
                        const ProtocolHandler& handler);
  bool UnregisterProtocol(const std::string& scheme);

  [[nodiscard]] const HandlersMap::mapped_type* FindRegistered(
      std::string_view scheme) const;

  bool InterceptProtocol(ProtocolType type,
                         const std::string& scheme,
                         const ProtocolHandler& handler);
  bool UninterceptProtocol(const std::string& scheme);

  [[nodiscard]] const HandlersMap::mapped_type* FindIntercepted(
      std::string_view scheme) const;

  // Schemes served by a handler running in a Node.js worker thread; see
  // shell/browser/net/worker_protocol.h. A scheme has at most one handler of
  // either kind.
  bool RegisterWorkerProtocol(const std::string& scheme,
                              scoped_refptr<WorkerProtocolEndpoint> endpoint);
  bool UnregisterWorkerProtocol(const std::string& scheme,
                                WorkerProtocolEndpoint* endpoint);
  bool IsRegistered(std::string_view scheme) const;
  // A factory serving `scheme` if a handler of either kind is registered for
  // it (not intercepted schemes), else an invalid remote.
  mojo::PendingRemote<network::mojom::URLLoaderFactory> CreateRegisteredFactory(
      std::string_view scheme) const;

 private:
  friend class ElectronBrowserContext;

  explicit ProtocolRegistry(ElectronBrowserContext* browser_context);

  const raw_ptr<ElectronBrowserContext> browser_context_;

  HandlersMap handlers_;
  HandlersMap intercept_handlers_;
  std::map<std::string, scoped_refptr<WorkerProtocolEndpoint>, std::less<>>
      worker_handlers_;
};

}  // namespace electron

#endif  // ELECTRON_SHELL_BROWSER_PROTOCOL_REGISTRY_H_
