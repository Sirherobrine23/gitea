// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package repo

import "sync"

type issueLiveTopic struct {
	repoID int64
	index  int64
}

var issueLiveRefreshHub = struct {
	sync.RWMutex
	subscribers map[issueLiveTopic]map[chan struct{}]struct{}
}{
	subscribers: make(map[issueLiveTopic]map[chan struct{}]struct{}),
}

func registerIssueLiveRefresh(repoID, index int64, refresh chan struct{}) func() {
	topic := issueLiveTopic{repoID: repoID, index: index}

	issueLiveRefreshHub.Lock()
	subscribers := issueLiveRefreshHub.subscribers[topic]
	if subscribers == nil {
		subscribers = make(map[chan struct{}]struct{})
		issueLiveRefreshHub.subscribers[topic] = subscribers
	}
	subscribers[refresh] = struct{}{}
	issueLiveRefreshHub.Unlock()

	return func() {
		issueLiveRefreshHub.Lock()
		if subscribers := issueLiveRefreshHub.subscribers[topic]; subscribers != nil {
			delete(subscribers, refresh)
			if len(subscribers) == 0 {
				delete(issueLiveRefreshHub.subscribers, topic)
			}
		}
		issueLiveRefreshHub.Unlock()
	}
}

func notifyIssueLive(repoID, index int64) {
	topic := issueLiveTopic{repoID: repoID, index: index}

	issueLiveRefreshHub.RLock()
	defer issueLiveRefreshHub.RUnlock()
	for refresh := range issueLiveRefreshHub.subscribers[topic] {
		select {
		case refresh <- struct{}{}:
		default:
		}
	}
}
