// Copyright 2016 Andrew Chernov
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package discovery

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/prometheus/common/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"

	etcd "github.com/coreos/etcd/client"
	"golang.org/x/net/context"
)

// MetricInfo contains info where monitoring gets metrics from node
type MetricInfo struct {
	URL  string      `json:"url"`
	Tags []MetricTag `json:"tags"`
}

// MetricTag tags for host using for monitoring
type MetricTag struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type EtcdDiscovery struct {
	client        etcd.Client
	kapi          etcd.KeysAPI
	nodes         map[string]string
	keyPrefix     string
	metricKey     string
	retryInterval time.Duration
}

// NewEtcdDiscovery returns a new EtcdDiscovery for the given config.
func NewEtcdDiscovery(conf *config.EtcdSDConfig) (*EtcdDiscovery, error) {
	etcdConf := etcd.Config{
		Endpoints: conf.Endpoints,
	}

	etcdClient, err := etcd.New(etcdConf)
	if err != nil {
		return nil, err
	}

	ed := &EtcdDiscovery{
		client:        etcdClient,
		kapi:          etcd.NewKeysAPI(etcdClient),
		nodes:         make(map[string]string),
		keyPrefix:     conf.KeyPrefix,
		metricKey:     conf.MetricKey,
		retryInterval: conf.RetryInterval,
	}

	return ed, nil
}

// Run implements the TargetProvider interface.
func (ed *EtcdDiscovery) Run(ctx context.Context, ch chan<- []*config.TargetGroup) {
	defer close(ch)

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(ed.retryInterval):
			if err := ed.watchNodes(ctx, ch); err != nil {
				log.Error(err.Error())
			}
		}
	}
}

func (ed *EtcdDiscovery) watchNodes(ctx context.Context, ch chan<- []*config.TargetGroup) error {
	index, err := ed.fetchNodes(ctx)
	if err != nil {
		return err
	}

	targetGroups, err := ed.makeTargetGroupsForAllNodes()
	if err != nil {
		return err
	}

	ch <- targetGroups

	// create watcher
	watcher := ed.kapi.Watcher(ed.keyPrefix, &etcd.WatcherOptions{
		AfterIndex: index,
		Recursive:  true,
	})

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		wresp, err := watcher.Next(ctx)
		if err != nil {
			return fmt.Errorf("etcd error: %s", err.Error())
		}

		var changedKey string
		switch wresp.Action {
		case "set", "update", "create", "compareAndSwap":
			v, exists := ed.nodes[wresp.Node.Key]
			if !exists || v != wresp.Node.Value {
				ed.nodes[wresp.Node.Key] = wresp.Node.Value
				changedKey = wresp.Node.Key
			}

		case "delete", "compareAndDelete", "expire":
			delete(ed.nodes, wresp.Node.Key)
			changedKey = wresp.Node.Key

		default:
			return fmt.Errorf("unknown action type: '%s'", wresp.Action)
		}

		if len(changedKey) == 0 {
			continue
		}

		tg, err := ed.makeTargetGroupForChangedKey(changedKey)
		if err != nil {
			return fmt.Errorf("error parse payload: %s", err.Error())
		}

		ch <- []*config.TargetGroup{tg}
	}
}

func (ed *EtcdDiscovery) fetchNodes(ctx context.Context) (uint64, error) {
	resp, err := ed.kapi.Get(ctx, ed.keyPrefix, &etcd.GetOptions{
		Recursive: true,
	})

	if err != nil {
		return 0, err
	}

	ed.getAllNodes(resp.Node)
	return resp.Index, nil
}

func (ed *EtcdDiscovery) getAllNodes(rootNode *etcd.Node) {
	for _, node := range rootNode.Nodes {
		if node.Dir {
			ed.getAllNodes(node)
			continue
		}

		if strings.Contains(node.Key, ed.metricKey) {
			ed.nodes[node.Key] = node.Value
		}
	}
}

func (ed *EtcdDiscovery) makeTargetGroupsForAllNodes() ([]*config.TargetGroup, error) {
	tgByKey := make(map[string]*config.TargetGroup)
	for key, value := range ed.nodes {
		tgKey := ed.makeTargetGroupKey(key)
		tg, exists := tgByKey[tgKey]
		if !exists {
			tg = &config.TargetGroup{
				Source: tgKey,
			}
			tgByKey[tgKey] = tg
		}

		var info MetricInfo
		if err := json.Unmarshal([]byte(value), &info); err != nil {
			return nil, err
		}

		if tg.Labels == nil {
			labels := make(model.LabelSet, 3)
			for _, tag := range info.Tags {
				labels[model.LabelName(tag.Key)] = model.LabelValue(tag.Value)
			}
			tg.Labels = labels
		}

		tg.Targets = append(tg.Targets, model.LabelSet{
			model.AddressLabel: model.LabelValue(info.URL),
		})
	}
	result := make([]*config.TargetGroup, 0, len(tgByKey))
	for _, tg := range tgByKey {
		result = append(result, tg)
	}
	return result, nil
}

func (ed *EtcdDiscovery) makeTargetGroupForChangedKey(changedKey string) (*config.TargetGroup, error) {
	changedTargetGroupKey := ed.makeTargetGroupKey(changedKey)
	tg := config.TargetGroup{
		Source: changedTargetGroupKey,
	}

	for key, value := range ed.nodes {
		tgKey := ed.makeTargetGroupKey(key)
		if tgKey != changedTargetGroupKey {
			continue
		}

		var info MetricInfo
		if err := json.Unmarshal([]byte(value), &info); err != nil {
			return nil, err
		}

		if tg.Labels == nil {
			labels := make(model.LabelSet, 3)
			for _, tag := range info.Tags {
				labels[model.LabelName(tag.Key)] = model.LabelValue(tag.Value)
			}
			tg.Labels = labels
		}

		tg.Targets = append(tg.Targets, model.LabelSet{
			model.AddressLabel: model.LabelValue(info.URL),
		})
	}
	return &tg, nil
}

func (ed *EtcdDiscovery) makeTargetGroupKey(key string) string {
	pos := strings.LastIndex(key, ed.metricKey)
	if pos > 0 {
		return key[:pos]
	}
	return key
}
