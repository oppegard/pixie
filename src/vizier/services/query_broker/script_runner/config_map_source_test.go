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
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"px.dev/pixie/src/utils"
)

const ConfigMapUID = "5b054918-d670-45f3-99d4-015bb07036b3"

func TestConfigMapScriptsSource(t *testing.T) {
	t.Run("returns the initial scripts from a configmap", func(t *testing.T) {
		configMap := cronScriptConfigMap()
		client := fake.NewSimpleClientset(configMap)
		source := NewConfigMapSource(client.CoreV1().ConfigMaps("pl"))
		upsertCh, deleteCh := mockSourceReceiver()
		err := source.Start(context.Background(), upsertCh, deleteCh)
		require.NoError(t, err)

		upsert := requireReceiveWithin(t, upsertCh, 1*time.Second)
		require.Equal(t, configMap.Data["script.pxl"], upsert.Script)
		require.Equal(t, configMap.Data["configs.yaml"], upsert.Configs)
		require.Equal(t, int64(1), upsert.FrequencyS)
	})

	t.Run("reports an error when it cannot watch configmaps", func(t *testing.T) {
		client := fake.NewSimpleClientset(cronScriptConfigMap())
		client.PrependWatchReactor("configmaps", func(_ k8stesting.Action) (bool, watch.Interface, error) {
			return true, nil, errors.New("could not watch")
		})
		err := NewConfigMapSource(client.CoreV1().ConfigMaps("pl")).Start(context.Background(), nil, nil)

		require.Error(t, err)
	})

	t.Run("reports an error when it cannot list configmaps", func(t *testing.T) {
		client := fake.NewSimpleClientset(cronScriptConfigMap())
		client.PrependReactor("list", "configmaps", func(_ k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, errors.New("could not list")
		})
		err := NewConfigMapSource(client.CoreV1().ConfigMaps("pl")).Start(context.Background(), nil, nil)
		require.Error(t, err)
	})

	t.Run("reports an error when it cannot parse a configmap", func(t *testing.T) {
		client := fake.NewSimpleClientset(cronScriptConfigMap())
		client.PrependReactor("list", "configmaps", func(_ k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, errors.New("could not list")
		})
		err := NewConfigMapSource(client.CoreV1().ConfigMaps("pl")).Start(context.Background(), nil, nil)
		require.Error(t, err)
	})

	t.Run("does not watch configmaps when it cannot list them", func(t *testing.T) {
		client := fake.NewSimpleClientset()
		client.PrependReactor("list", "configmaps", func(_ k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, errors.New("could not list")
		})
		upsertCh, deleteCh := mockSourceReceiver()
		_ = NewConfigMapSource(client.CoreV1().ConfigMaps("pl")).Start(context.Background(), upsertCh, deleteCh)

		_, _ = client.CoreV1().ConfigMaps("pl").Create(context.Background(), cronScriptConfigMap(), metav1.CreateOptions{})

		requireNoReceive(t, upsertCh, 10*time.Millisecond)
	})

	t.Run("stop causes no further updates to be sent", func(t *testing.T) {
		client := fake.NewSimpleClientset()
		upsertCh, deleteCh := mockSourceReceiver()
		syncConfigMaps := NewConfigMapSource(client.CoreV1().ConfigMaps("pl"))
		_ = syncConfigMaps.Start(context.Background(), upsertCh, deleteCh)
		syncConfigMaps.Stop()

		_, _ = client.CoreV1().ConfigMaps("pl").Create(context.Background(), cronScriptConfigMap(), metav1.CreateOptions{})

		requireNoReceive(t, upsertCh, 10*time.Millisecond)
	})

	t.Run("only reports configmaps that have purpose=cron-script labels", func(t *testing.T) {
		client := fake.NewSimpleClientset(&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				UID:       "66b04d98-efbc-45e3-a805-b7c1d37e72fc",
				Name:      "invalid-cron-script",
				Namespace: "pl",
				Labels:    map[string]string{},
			},
			Data: map[string]string{
				"script.pxl":   "px.display()",
				"configs.yaml": "otelEndpointConfig: {url: example.com}",
				"cron.yaml":    "frequency_s: 1",
			},
		})
		source := NewConfigMapSource(client.CoreV1().ConfigMaps("pl"))
		upsertCh, deleteCh := mockSourceReceiver()
		_ = source.Start(context.Background(), upsertCh, deleteCh)

		requireNoReceive(t, upsertCh, 10*time.Millisecond)
	})

	t.Run("updates with newly added scripts", func(t *testing.T) {
		client := fake.NewSimpleClientset()
		upsertCh, deleteCh := mockSourceReceiver()
		_ = NewConfigMapSource(client.CoreV1().ConfigMaps("pl")).Start(context.Background(), upsertCh, deleteCh)

		_, _ = client.CoreV1().ConfigMaps("pl").Create(context.Background(), cronScriptConfigMap(), metav1.CreateOptions{})

		upsert := requireReceiveWithin(t, upsertCh, time.Second)
		require.Equal(t, utils.ProtoFromUUIDStrOrNil(ConfigMapUID), upsert.GetID())
		require.Equal(t, "px.display()", upsert.GetScript())
		require.Equal(t, "otelEndpointConfig: {url: example.com}", upsert.GetConfigs())
		require.Equal(t, int64(1), upsert.GetFrequencyS())
	})

	t.Run("updates existing scripts", func(t *testing.T) {
		configMapTemplate := cronScriptConfigMap()
		client := fake.NewSimpleClientset(configMapTemplate)
		upsertCh, deleteCh := mockSourceReceiver()
		_ = NewConfigMapSource(client.CoreV1().ConfigMaps("pl")).Start(context.Background(), upsertCh, deleteCh)

		configMapTemplate.Data["script.pxl"] = "px.display2()"
		_, _ = client.CoreV1().ConfigMaps("pl").Update(context.Background(), configMapTemplate, metav1.UpdateOptions{})

		requireReceiveWithin(t, upsertCh, time.Second)
		upsert := requireReceiveWithin(t, upsertCh, time.Second)
		require.Equal(t, utils.ProtoFromUUIDStrOrNil(ConfigMapUID), upsert.GetID())
		require.Equal(t, "px.display2()", upsert.GetScript())
		require.Equal(t, "otelEndpointConfig: {url: example.com}", upsert.GetConfigs())
		require.Equal(t, int64(1), upsert.GetFrequencyS())
	})

	t.Run("excludes changes to configmaps that don't have purpose=cron-script labels", func(t *testing.T) {
		watchLabelSelector := labels.NewSelector()
		client := fake.NewSimpleClientset()
		client.PrependWatchReactor("configmaps", func(action k8stesting.Action) (bool, watch.Interface, error) {
			watchLabelSelector = action.(k8stesting.WatchActionImpl).WatchRestrictions.Labels
			return false, nil, nil
		})
		upsertCh, deleteCh := mockSourceReceiver()
		_ = NewConfigMapSource(client.CoreV1().ConfigMaps("pl")).Start(context.Background(), upsertCh, deleteCh)

		require.False(t, watchLabelSelector.Matches(labels.Set(map[string]string{})))
		require.True(t, watchLabelSelector.Matches(labels.Set(map[string]string{"purpose": "cron-script"})))
	})

	t.Run("updates when a config map cron script is deleted", func(t *testing.T) {
		configMapTemplate := cronScriptConfigMap()
		client := fake.NewSimpleClientset(configMapTemplate)
		upsertCh, deleteCh := mockSourceReceiver()
		_ = NewConfigMapSource(client.CoreV1().ConfigMaps("pl")).Start(context.Background(), upsertCh, deleteCh)

		configMapTemplate.Data["script.pxl"] = "px.display2()"
		_ = client.CoreV1().ConfigMaps("pl").Delete(context.Background(), configMapTemplate.GetName(), metav1.DeleteOptions{})

		deletedID := requireReceiveWithin(t, deleteCh, time.Second)
		require.Equal(t, ConfigMapUID, deletedID.String())
	})
}

func cronScriptConfigMap() *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			UID:       ConfigMapUID,
			Name:      "cron-script-1",
			Namespace: "pl",
			Labels:    map[string]string{"purpose": "cron-script"},
		},
		Data: map[string]string{
			"script.pxl":   "px.display()",
			"configs.yaml": "otelEndpointConfig: {url: example.com}",
			"cron.yaml":    "frequency_s: 1",
		},
	}
}
