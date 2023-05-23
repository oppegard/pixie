/*
 * Copyright 2018- The Pixie Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package scriptrunner

import (
	"context"

	"github.com/gofrs/uuid"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	clientv1 "k8s.io/client-go/kubernetes/typed/core/v1"

	"px.dev/pixie/src/shared/cvmsgspb"
	"px.dev/pixie/src/utils"
)

// ConfigMapSource pulls cron scripts from config maps.
type ConfigMapSource struct {
	stop   func()
	client clientv1.ConfigMapInterface
}

// NewConfigMapSource constructs a [Source] that extracts cron scripts from config maps with the label "purpose=cron-script".
// Each config map must contain
//   - a script.pxl with the pixel script
//   - a configs.yaml which will be stored in the Configs field of [cvmsgspb.CronScript]
//   - a cron.yaml that contains a "frequency_s" key
func NewConfigMapSource(client clientv1.ConfigMapInterface) *ConfigMapSource {
	return &ConfigMapSource{client: client}
}

// Start watches for updates to matching configmaps and sends resulting updates on upsertCh.
func (source *ConfigMapSource) Start(baseCtx context.Context, upsertCh chan *cvmsgspb.CronScript, deleteCh chan uuid.UUID) error {
	options := metav1.ListOptions{LabelSelector: "purpose=cron-script"}
	watcher, err := source.client.Watch(baseCtx, options)
	if err != nil {
		return err
	}
	go configMapUpdater(watcher, upsertCh, deleteCh)
	configmaps, err := source.client.List(baseCtx, options)
	if err != nil {
		watcher.Stop()
		return err
	}
	for _, configmap := range configmaps.Items {
		_, script, err := configmapToCronScript(&configmap)
		if err != nil {
			logCronScriptParseError(err)
			continue
		}
		upsertCh <- script
	}
	source.stop = watcher.Stop
	return nil
}

// Stop stops further updates from being sent.
func (source *ConfigMapSource) Stop() {
	source.stop()
}

func configMapUpdater(watcher watch.Interface, upsertCh chan *cvmsgspb.CronScript, deleteCh chan uuid.UUID) {
	for event := range watcher.ResultChan() {
		switch event.Type {
		case watch.Modified, watch.Added:
			configmap := event.Object.(*v1.ConfigMap)
			_, script, err := configmapToCronScript(configmap)
			if err != nil {
				logCronScriptParseError(err)
				continue
			}
			upsertCh <- script
		case watch.Deleted:
			configmap := event.Object.(*v1.ConfigMap)
			id, _, err := configmapToCronScript(configmap)
			if err != nil {
				logCronScriptParseError(err)
				continue
			}
			deleteCh <- id
		}
	}
}

func logCronScriptParseError(err error) {
	log.WithError(err).Error("Failed to parse cron.yaml from configmap cron script")
}

func configmapToCronScript(configmap *v1.ConfigMap) (uuid.UUID, *cvmsgspb.CronScript, error) {
	id := string(configmap.UID)
	cronScript := &cvmsgspb.CronScript{
		ID:      utils.ProtoFromUUIDStrOrNil(id),
		Script:  configmap.Data["script.pxl"],
		Configs: configmap.Data["configs.yaml"],
	}
	var cronData cronYAML
	err := yaml.Unmarshal([]byte(configmap.Data["cron.yaml"]), &cronData)
	if err != nil {
		return uuid.Nil, nil, err
	}
	cronScript.FrequencyS = cronData.FrequencyS
	return uuid.FromStringOrNil(id), cronScript, nil
}

type cronYAML struct {
	FrequencyS int64 `yaml:"frequency_s"`
}
