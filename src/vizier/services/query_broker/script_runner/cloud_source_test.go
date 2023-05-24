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
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/gofrs/uuid"
	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"px.dev/pixie/src/shared/cvmsgspb"
	"px.dev/pixie/src/shared/scripts"
	"px.dev/pixie/src/utils"
	"px.dev/pixie/src/utils/testingutils"
	"px.dev/pixie/src/vizier/services/metadata/metadatapb"
)

func TestCloudScriptsSource_InitialState(t *testing.T) {
	tests := []struct {
		name             string
		persistedScripts map[string]*cvmsgspb.CronScript
		cloudScripts     map[string]*cvmsgspb.CronScript
		checksumMatch    bool
	}{
		{
			name: "checksums match",
			persistedScripts: map[string]*cvmsgspb.CronScript{
				"223e4567-e89b-12d3-a456-426655440000": {
					ID:         utils.ProtoFromUUIDStrOrNil("223e4567-e89b-12d3-a456-426655440000"),
					Script:     "px.display()",
					Configs:    "config1",
					FrequencyS: 5,
				},
				"223e4567-e89b-12d3-a456-426655440001": {
					ID:         utils.ProtoFromUUIDStrOrNil("223e4567-e89b-12d3-a456-426655440001"),
					Script:     "test script",
					Configs:    "config2",
					FrequencyS: 22,
				},
			},
			cloudScripts: map[string]*cvmsgspb.CronScript{
				"223e4567-e89b-12d3-a456-426655440000": {
					ID:         utils.ProtoFromUUIDStrOrNil("223e4567-e89b-12d3-a456-426655440000"),
					Script:     "px.display()",
					Configs:    "config1",
					FrequencyS: 5,
				},
				"223e4567-e89b-12d3-a456-426655440001": {
					ID:         utils.ProtoFromUUIDStrOrNil("223e4567-e89b-12d3-a456-426655440001"),
					Script:     "test script",
					Configs:    "config2",
					FrequencyS: 22,
				},
			},
			checksumMatch: true,
		},
		{
			name: "checksums mismatch: one field different",
			persistedScripts: map[string]*cvmsgspb.CronScript{
				"223e4567-e89b-12d3-a456-426655440000": {
					ID:         utils.ProtoFromUUIDStrOrNil("223e4567-e89b-12d3-a456-426655440000"),
					Script:     "px.display()",
					Configs:    "config1",
					FrequencyS: 5,
				},
				"223e4567-e89b-12d3-a456-426655440001": {
					ID:         utils.ProtoFromUUIDStrOrNil("223e4567-e89b-12d3-a456-426655440001"),
					Script:     "test script",
					Configs:    "config2",
					FrequencyS: 22,
				},
			},
			cloudScripts: map[string]*cvmsgspb.CronScript{
				"223e4567-e89b-12d3-a456-426655440000": {
					ID:         utils.ProtoFromUUIDStrOrNil("223e4567-e89b-12d3-a456-426655440000"),
					Script:     "px.display()",
					Configs:    "config1",
					FrequencyS: 6,
				},
				"223e4567-e89b-12d3-a456-426655440001": {
					ID:         utils.ProtoFromUUIDStrOrNil("223e4567-e89b-12d3-a456-426655440001"),
					Script:     "test script",
					Configs:    "config2",
					FrequencyS: 22,
				},
			},
			checksumMatch: false,
		},
		{
			name: "checksums mismatch: missing script",
			persistedScripts: map[string]*cvmsgspb.CronScript{
				"223e4567-e89b-12d3-a456-426655440000": {
					ID:         utils.ProtoFromUUIDStrOrNil("223e4567-e89b-12d3-a456-426655440000"),
					Script:     "px.display()",
					Configs:    "config1",
					FrequencyS: 5,
				},
				"223e4567-e89b-12d3-a456-426655440001": {
					ID:         utils.ProtoFromUUIDStrOrNil("223e4567-e89b-12d3-a456-426655440001"),
					Script:     "test script",
					Configs:    "config2",
					FrequencyS: 22,
				},
			},
			cloudScripts: map[string]*cvmsgspb.CronScript{
				"223e4567-e89b-12d3-a456-426655440000": {
					ID:         utils.ProtoFromUUIDStrOrNil("223e4567-e89b-12d3-a456-426655440000"),
					Script:     "px.display()",
					Configs:    "config1",
					FrequencyS: 6,
				},
			},
			checksumMatch: false,
		},
		{
			name:             "checksums match: empty",
			persistedScripts: map[string]*cvmsgspb.CronScript{},
			cloudScripts:     map[string]*cvmsgspb.CronScript{},
			checksumMatch:    true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			nc, natsCleanup := testingutils.MustStartTestNATS(t)
			defer natsCleanup()
			persistedScripts := map[uuid.UUID]*cvmsgspb.CronScript{}
			for id, script := range test.persistedScripts {
				persistedScripts[uuid.Must(uuid.FromString(id))] = script
			}
			fcs := &fakeCronStore{scripts: persistedScripts}

			cloudScripts := test.cloudScripts
			checksumSub, _ := setupChecksumSubscription(t, nc, cloudScripts)
			defer func() {
				require.NoError(t, checksumSub.Unsubscribe())
			}()

			scriptSub, gotCronScripts := setupCloudScriptsSubscription(t, nc, cloudScripts)
			defer func() {
				require.NoError(t, scriptSub.Unsubscribe())
			}()

			source := NewCloudSource(nc, fcs, "test")
			upsertCh, deleteCh := mockSourceReceiver()
			err := source.Start(context.Background(), upsertCh, deleteCh)
			require.NoError(t, err)
			defer source.Stop()

			select {
			case <-gotCronScripts:
				if test.checksumMatch {
					t.Fatal("should not have fetched cron scripts")
				}
			case <-time.After(time.Millisecond):
				if !test.checksumMatch {
					t.Fatal("should have fetched cron scripts")
				}
			}
		})
	}

	t.Run("does not receive updates after the metadata store fails to fetch the initial state", func(t *testing.T) {
		nc, natsCleanup := testingutils.MustStartTestNATS(t)
		defer natsCleanup()
		scs := &stubCronScriptStore{GetScriptsError: errors.New("failed to get scripts")}

		checksumSub, _ := setupChecksumSubscription(t, nc, nil)
		defer func() {
			require.NoError(t, checksumSub.Unsubscribe())
		}()

		sentUpdates := []*cvmsgspb.CronScriptUpdate{
			{
				Msg: &cvmsgspb.CronScriptUpdate_UpsertReq{
					UpsertReq: &cvmsgspb.RegisterOrUpdateCronScriptRequest{
						Script: &cvmsgspb.CronScript{
							ID:         utils.ProtoFromUUIDStrOrNil("223e4567-e89b-12d3-a456-426655440002"),
							Script:     "test script 1",
							Configs:    "config1",
							FrequencyS: 123,
						},
					},
				},
				RequestID: "1",
				Timestamp: 1,
			},
		}
		cronScriptResSubs, gotCronScriptResponses := setupCronScriptResponses(t, nc, sentUpdates)
		defer func() {
			for _, sub := range cronScriptResSubs {
				require.NoError(t, sub.Unsubscribe())
			}
		}()

		upsertCh, deleteCh := mockSourceReceiver()
		source := NewCloudSource(nc, scs, "test")
		err := source.Start(context.Background(), upsertCh, deleteCh)
		require.Error(t, err)

		sendUpdates(t, nc, sentUpdates)

		requireNoReceive(t, upsertCh, time.Millisecond)
		for _, getCronScriptResponse := range gotCronScriptResponses {
			requireNoReceive(t, getCronScriptResponse, time.Millisecond)
		}
	})

	t.Run("does not receive updates after the metadata store fails to save the initial state", func(t *testing.T) {
		persistedScripts := map[string]*cvmsgspb.CronScript{
			"223e4567-e89b-12d3-a456-426655440000": {
				ID:         utils.ProtoFromUUIDStrOrNil("223e4567-e89b-12d3-a456-426655440000"),
				Script:     "px.display()",
				Configs:    "config1",
				FrequencyS: 5,
			},
			"223e4567-e89b-12d3-a456-426655440001": {
				ID:         utils.ProtoFromUUIDStrOrNil("223e4567-e89b-12d3-a456-426655440001"),
				Script:     "test script",
				Configs:    "config2",
				FrequencyS: 22,
			},
		}
		cloudScripts := map[string]*cvmsgspb.CronScript{
			"223e4567-e89b-12d3-a456-426655440000": {
				ID:         utils.ProtoFromUUIDStrOrNil("223e4567-e89b-12d3-a456-426655440000"),
				Script:     "px.display()",
				Configs:    "config1",
				FrequencyS: 6,
			},
			"223e4567-e89b-12d3-a456-426655440001": {
				ID:         utils.ProtoFromUUIDStrOrNil("223e4567-e89b-12d3-a456-426655440001"),
				Script:     "test script",
				Configs:    "config2",
				FrequencyS: 22,
			},
		}
		nc, natsCleanup := testingutils.MustStartTestNATS(t)
		defer natsCleanup()
		scs := &stubCronScriptStore{
			GetScriptsResponse: &metadatapb.GetScriptsResponse{Scripts: persistedScripts},
			SetScriptsError:    errors.New("could not save scripts"),
		}

		checksumSub, _ := setupChecksumSubscription(t, nc, cloudScripts)
		defer func() {
			require.NoError(t, checksumSub.Unsubscribe())
		}()

		scriptSub, _ := setupCloudScriptsSubscription(t, nc, cloudScripts)
		defer func() {
			require.NoError(t, scriptSub.Unsubscribe())
		}()

		sentUpdates := []*cvmsgspb.CronScriptUpdate{
			{
				Msg: &cvmsgspb.CronScriptUpdate_UpsertReq{
					UpsertReq: &cvmsgspb.RegisterOrUpdateCronScriptRequest{
						Script: &cvmsgspb.CronScript{
							ID:         utils.ProtoFromUUIDStrOrNil("223e4567-e89b-12d3-a456-426655440002"),
							Script:     "test script 1",
							Configs:    "config1",
							FrequencyS: 123,
						},
					},
				},
				RequestID: "1",
				Timestamp: 1,
			},
		}
		cronScriptResSubs, gotCronScriptResponses := setupCronScriptResponses(t, nc, sentUpdates)
		defer func() {
			for _, sub := range cronScriptResSubs {
				require.NoError(t, sub.Unsubscribe())
			}
		}()

		upsertCh, deleteCh := mockSourceReceiver()
		source := NewCloudSource(nc, scs, "test")
		err := source.Start(context.Background(), upsertCh, deleteCh)
		require.Error(t, err)

		sendUpdates(t, nc, sentUpdates)

		requireNoReceive(t, upsertCh, time.Millisecond)
		for _, getCronScriptResponse := range gotCronScriptResponses {
			requireNoReceive(t, getCronScriptResponse, time.Millisecond)
		}
	})
}

func TestCloudScriptsSource_Updates(t *testing.T) {
	tests := []struct {
		name    string
		scripts map[string]*cvmsgspb.CronScript
		updates []*cvmsgspb.CronScriptUpdate
	}{
		{
			name: "add one, delete one",
			scripts: map[string]*cvmsgspb.CronScript{
				"223e4567-e89b-12d3-a456-426655440001": {
					ID:         utils.ProtoFromUUIDStrOrNil("223e4567-e89b-12d3-a456-426655440001"),
					Script:     "test script 1",
					Configs:    "config1",
					FrequencyS: 123,
				},
			},
			updates: []*cvmsgspb.CronScriptUpdate{
				{
					Msg: &cvmsgspb.CronScriptUpdate_UpsertReq{
						UpsertReq: &cvmsgspb.RegisterOrUpdateCronScriptRequest{
							Script: &cvmsgspb.CronScript{
								ID:         utils.ProtoFromUUIDStrOrNil("223e4567-e89b-12d3-a456-426655440002"),
								Script:     "test script 2",
								Configs:    "config2",
								FrequencyS: 123,
							},
						},
					},
					RequestID: "1",
					Timestamp: 1,
				},
				{
					Msg: &cvmsgspb.CronScriptUpdate_DeleteReq{
						DeleteReq: &cvmsgspb.DeleteCronScriptRequest{
							ScriptID: utils.ProtoFromUUIDStrOrNil("223e4567-e89b-12d3-a456-426655440001"),
						},
					},
					RequestID: "2",
					Timestamp: 2,
				},
			},
		},
		{
			name: "update one, delete one",
			scripts: map[string]*cvmsgspb.CronScript{
				"223e4567-e89b-12d3-a456-426655440002": {
					ID:         utils.ProtoFromUUIDStrOrNil("223e4567-e89b-12d3-a456-426655440002"),
					Script:     "test script 1",
					Configs:    "config1",
					FrequencyS: 123,
				},
				"223e4567-e89b-12d3-a456-426655440001": {
					ID:         utils.ProtoFromUUIDStrOrNil("223e4567-e89b-12d3-a456-426655440001"),
					Script:     "test script 1",
					Configs:    "config1",
					FrequencyS: 123,
				},
			},
			updates: []*cvmsgspb.CronScriptUpdate{
				{
					Msg: &cvmsgspb.CronScriptUpdate_UpsertReq{
						UpsertReq: &cvmsgspb.RegisterOrUpdateCronScriptRequest{
							Script: &cvmsgspb.CronScript{
								ID:         utils.ProtoFromUUIDStrOrNil("223e4567-e89b-12d3-a456-426655440002"),
								Script:     "test script 2",
								Configs:    "config2",
								FrequencyS: 123,
							},
						},
					},
					RequestID: "1",
					Timestamp: 1,
				},
				{
					Msg: &cvmsgspb.CronScriptUpdate_DeleteReq{
						DeleteReq: &cvmsgspb.DeleteCronScriptRequest{
							ScriptID: utils.ProtoFromUUIDStrOrNil("223e4567-e89b-12d3-a456-426655440001"),
						},
					},
					RequestID: "2",
					Timestamp: 2,
				},
			},
		},
		{
			name:    "no scripts",
			scripts: map[string]*cvmsgspb.CronScript{},
			updates: []*cvmsgspb.CronScriptUpdate{
				{
					Msg: &cvmsgspb.CronScriptUpdate_UpsertReq{
						UpsertReq: &cvmsgspb.RegisterOrUpdateCronScriptRequest{
							Script: &cvmsgspb.CronScript{
								ID:         utils.ProtoFromUUIDStrOrNil("223e4567-e89b-12d3-a456-426655440002"),
								Script:     "test script 2",
								Configs:    "config3",
								FrequencyS: 123,
							},
						},
					},
					RequestID: "1",
					Timestamp: 1,
				},
				{
					Msg: &cvmsgspb.CronScriptUpdate_DeleteReq{
						DeleteReq: &cvmsgspb.DeleteCronScriptRequest{
							ScriptID: utils.ProtoFromUUIDStrOrNil("223e4567-e89b-12d3-a456-426655440003"),
						},
					},
					RequestID: "2",
					Timestamp: 2,
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			nc, natsCleanup := testingutils.MustStartTestNATS(t)
			defer natsCleanup()
			persistedScripts := map[uuid.UUID]*cvmsgspb.CronScript{}
			for id, script := range test.scripts {
				persistedScripts[uuid.Must(uuid.FromString(id))] = script
			}
			fcs := &fakeCronStore{scripts: persistedScripts}

			checksumSub, gotChecksumReq := setupChecksumSubscription(t, nc, test.scripts)
			defer func() {
				require.NoError(t, checksumSub.Unsubscribe())
			}()

			cronScriptResSubs, gotCronScriptResponses := setupCronScriptResponses(t, nc, test.updates)
			defer func() {
				for _, sub := range cronScriptResSubs {
					require.NoError(t, sub.Unsubscribe())
				}
			}()

			upsertCh, deleteCh := mockSourceReceiver()
			source := NewCloudSource(nc, fcs, "test")
			err := source.Start(context.Background(), upsertCh, deleteCh)
			require.NoError(t, err)
			defer source.Stop()

			<-gotChecksumReq
			var expectedInitialUpserts []*cvmsgspb.CronScript
			for _, initial := range test.scripts {
				expectedInitialUpserts = append(expectedInitialUpserts, initial)
			}
			missingInitialUpserts, unexpectedInitialUpserts, _ := diffMessages(expectedInitialUpserts, upsertCh)
			require.Empty(t, missingInitialUpserts, "missing upserts")
			require.Empty(t, unexpectedInitialUpserts, "unexpected upserts")

			sendUpdates(t, nc, test.updates)

			var expectedUpserts []*cvmsgspb.CronScript
			var expectedDeletes []uuid.UUID
			for _, update := range test.updates {
				switch update.Msg.(type) {
				case *cvmsgspb.CronScriptUpdate_UpsertReq:
					expectedUpserts = append(expectedUpserts, update.GetUpsertReq().Script)
				case *cvmsgspb.CronScriptUpdate_DeleteReq:
					expectedDeletes = append(expectedDeletes, utils.UUIDFromProtoOrNil(update.GetDeleteReq().ScriptID))
				}
			}

			missingUpserts, unexpectedUpserts, allUpserts := diffMessages(expectedUpserts, upsertCh)
			require.Empty(t, missingUpserts, "missing upserts")
			require.Empty(t, unexpectedUpserts, "unexpected upserts")

			missingDeletes, unexpectedDeletes, allDeletes := diffMessages(expectedDeletes, deleteCh)
			require.Empty(t, missingDeletes, "missing deletes")
			require.Empty(t, unexpectedDeletes, "unexpected deletes")

			for _, id := range allDeletes {
				requireReceiveWithin(t, gotCronScriptResponses[id], time.Second, id.String())
				require.NotContains(t, fcs.scripts, id)
			}

			for _, update := range allUpserts {
				id := utils.UUIDFromProtoOrNil(update.ID)
				requireReceiveWithin(t, gotCronScriptResponses[id], time.Second, id.String())
				require.Contains(t, fcs.scripts, id)
			}
		})
	}

	t.Run("does not send any further updates after stopping", func(t *testing.T) {
		nc, natsCleanup := testingutils.MustStartTestNATS(t)
		defer natsCleanup()
		fcs := &fakeCronStore{scripts: nil}

		checksumSub, _ := setupChecksumSubscription(t, nc, nil)
		defer func() {
			require.NoError(t, checksumSub.Unsubscribe())
		}()

		sentUpdates := []*cvmsgspb.CronScriptUpdate{
			{
				Msg: &cvmsgspb.CronScriptUpdate_UpsertReq{
					UpsertReq: &cvmsgspb.RegisterOrUpdateCronScriptRequest{
						Script: &cvmsgspb.CronScript{
							ID:         utils.ProtoFromUUIDStrOrNil("223e4567-e89b-12d3-a456-426655440002"),
							Script:     "test script 1",
							Configs:    "config1",
							FrequencyS: 123,
						},
					},
				},
				RequestID: "1",
				Timestamp: 1,
			},
		}
		cronScriptResSubs, gotCronScriptResponses := setupCronScriptResponses(t, nc, sentUpdates)
		defer func() {
			for _, sub := range cronScriptResSubs {
				require.NoError(t, sub.Unsubscribe())
			}
		}()

		upsertCh, deleteCh := mockSourceReceiver()
		source := NewCloudSource(nc, fcs, "test")
		err := source.Start(context.Background(), upsertCh, deleteCh)
		require.NoError(t, err)
		source.Stop()

		sendUpdates(t, nc, sentUpdates)

		requireNoReceive(t, upsertCh, time.Millisecond)
		requireNoReceive(t, deleteCh, time.Millisecond)
		for _, getCronScriptResponse := range gotCronScriptResponses {
			requireNoReceive(t, getCronScriptResponse, time.Millisecond)
		}
	})
}

func TestCloudScriptsSource_UpdateOrdering(t *testing.T) {
	tests := []struct {
		name            string
		scripts         map[string]*cvmsgspb.CronScript
		updates         []*cvmsgspb.CronScriptUpdate
		expectedUpserts int
		expectedDeletes int
	}{
		{
			name: "updates out of order",
			updates: []*cvmsgspb.CronScriptUpdate{
				{
					Msg: &cvmsgspb.CronScriptUpdate_UpsertReq{
						UpsertReq: &cvmsgspb.RegisterOrUpdateCronScriptRequest{
							Script: &cvmsgspb.CronScript{
								ID:         utils.ProtoFromUUIDStrOrNil("223e4567-e89b-12d3-a456-426655440001"),
								Script:     "test script 2",
								Configs:    "config2",
								FrequencyS: 123,
							},
						},
					},
					RequestID: "2",
					Timestamp: 2,
				},
				{
					Msg: &cvmsgspb.CronScriptUpdate_UpsertReq{
						UpsertReq: &cvmsgspb.RegisterOrUpdateCronScriptRequest{
							Script: &cvmsgspb.CronScript{
								ID:         utils.ProtoFromUUIDStrOrNil("223e4567-e89b-12d3-a456-426655440001"),
								Script:     "test script 1",
								Configs:    "config1",
								FrequencyS: 123,
							},
						},
					},
					RequestID: "1",
					Timestamp: 1,
				},
			},
			expectedUpserts: 1,
			expectedDeletes: 0,
		},
		{
			name: "deletes out of order",
			scripts: map[string]*cvmsgspb.CronScript{
				"223e4567-e89b-12d3-a456-426655440001": {
					ID:         utils.ProtoFromUUIDStrOrNil("223e4567-e89b-12d3-a456-426655440001"),
					Script:     "test script 1",
					Configs:    "config1",
					FrequencyS: 123,
				},
			},
			updates: []*cvmsgspb.CronScriptUpdate{
				{
					Msg: &cvmsgspb.CronScriptUpdate_DeleteReq{
						DeleteReq: &cvmsgspb.DeleteCronScriptRequest{
							ScriptID: utils.ProtoFromUUIDStrOrNil("223e4567-e89b-12d3-a456-426655440001"),
						},
					},
					RequestID: "2",
					Timestamp: 2,
				},
				{
					Msg: &cvmsgspb.CronScriptUpdate_DeleteReq{
						DeleteReq: &cvmsgspb.DeleteCronScriptRequest{
							ScriptID: utils.ProtoFromUUIDStrOrNil("223e4567-e89b-12d3-a456-426655440001"),
						},
					},
					RequestID: "1",
					Timestamp: 1,
				},
			},
			expectedUpserts: 0,
			expectedDeletes: 1,
		},
		{
			name: "old update after delete",
			scripts: map[string]*cvmsgspb.CronScript{
				"223e4567-e89b-12d3-a456-426655440001": {
					ID:         utils.ProtoFromUUIDStrOrNil("223e4567-e89b-12d3-a456-426655440001"),
					Script:     "test script 1",
					Configs:    "config1",
					FrequencyS: 123,
				},
			},
			updates: []*cvmsgspb.CronScriptUpdate{
				{
					Msg: &cvmsgspb.CronScriptUpdate_DeleteReq{
						DeleteReq: &cvmsgspb.DeleteCronScriptRequest{
							ScriptID: utils.ProtoFromUUIDStrOrNil("223e4567-e89b-12d3-a456-426655440001"),
						},
					},
					RequestID: "2",
					Timestamp: 2,
				},
				{
					Msg: &cvmsgspb.CronScriptUpdate_UpsertReq{
						UpsertReq: &cvmsgspb.RegisterOrUpdateCronScriptRequest{
							Script: &cvmsgspb.CronScript{
								ID:         utils.ProtoFromUUIDStrOrNil("223e4567-e89b-12d3-a456-426655440001"),
								Script:     "test script 2",
								Configs:    "config2",
								FrequencyS: 123,
							},
						},
					},
					RequestID: "1",
					Timestamp: 1,
				},
			},
			expectedUpserts: 0,
			expectedDeletes: 1,
		},
		{
			name: "old delete after update",
			scripts: map[string]*cvmsgspb.CronScript{
				"223e4567-e89b-12d3-a456-426655440001": {
					ID:         utils.ProtoFromUUIDStrOrNil("223e4567-e89b-12d3-a456-426655440001"),
					Script:     "test script 1",
					Configs:    "config1",
					FrequencyS: 123,
				},
			},
			updates: []*cvmsgspb.CronScriptUpdate{
				{
					Msg: &cvmsgspb.CronScriptUpdate_UpsertReq{
						UpsertReq: &cvmsgspb.RegisterOrUpdateCronScriptRequest{
							Script: &cvmsgspb.CronScript{
								ID:         utils.ProtoFromUUIDStrOrNil("223e4567-e89b-12d3-a456-426655440001"),
								Script:     "test script 2",
								Configs:    "config2",
								FrequencyS: 123,
							},
						},
					},
					RequestID: "2",
					Timestamp: 2,
				},
				{
					Msg: &cvmsgspb.CronScriptUpdate_DeleteReq{
						DeleteReq: &cvmsgspb.DeleteCronScriptRequest{
							ScriptID: utils.ProtoFromUUIDStrOrNil("223e4567-e89b-12d3-a456-426655440001"),
						},
					},
					RequestID: "1",
					Timestamp: 1,
				},
			},
			expectedUpserts: 1,
			expectedDeletes: 0,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			nc, natsCleanup := testingutils.MustStartTestNATS(t)
			defer natsCleanup()
			persistedScripts := map[uuid.UUID]*cvmsgspb.CronScript{}
			for id, script := range test.scripts {
				persistedScripts[uuid.Must(uuid.FromString(id))] = script
			}
			fcs := &fakeCronStore{scripts: persistedScripts}

			checksumSub, gotChecksumReq := setupChecksumSubscription(t, nc, test.scripts)
			defer func() {
				require.NoError(t, checksumSub.Unsubscribe())
			}()

			cronScriptResSubs, _ := setupCronScriptResponses(t, nc, test.updates)
			defer func() {
				for _, sub := range cronScriptResSubs {
					require.NoError(t, sub.Unsubscribe())
				}
			}()

			upsertCh, deleteCh := mockSourceReceiver()
			source := NewCloudSource(nc, fcs, "test")
			err := source.Start(context.Background(), upsertCh, deleteCh)
			require.NoError(t, err)
			defer source.Stop()

			<-gotChecksumReq
			var expectedInitialUpserts []*cvmsgspb.CronScript
			for _, initial := range test.scripts {
				expectedInitialUpserts = append(expectedInitialUpserts, initial)
			}
			missingInitialUpserts, unexpectedInitialUpserts, _ := diffMessages(expectedInitialUpserts, upsertCh)
			require.Empty(t, missingInitialUpserts, "missing upserts")
			require.Empty(t, unexpectedInitialUpserts, "unexpected upserts")

			sendUpdates(t, nc, test.updates)

			actualUpserts := countAll(upsertCh, time.Millisecond)
			require.Equalf(t, test.expectedUpserts, actualUpserts, "expected %d upserts, but got %d", test.expectedUpserts, actualUpserts)

			actualDeletes := countAll(deleteCh, time.Millisecond)
			require.Equalf(t, test.expectedDeletes, actualDeletes, "expected %d deletes, but got %d", test.expectedUpserts, actualDeletes)
		})
	}
}

func countAll[T any](msgs chan T, timeout time.Duration) int {
	count := 0
	for {
		select {
		case <-msgs:
			count++
		case <-time.After(timeout):
			return count
		}
	}
}

func diffMessages[T any](expected []T, msgs chan T) (missing []T, unexpected []T, all []T) {
	for len(expected) > 0 {
		select {
		case actual := <-msgs:
			all = append(all, actual)
			anyMatches := false
			for i := 0; i < len(expected); i++ {
				if reflect.DeepEqual(expected[i], actual) {
					expected[i] = expected[len(expected)-1]
					expected = expected[:len(expected)-1]
					anyMatches = true
					break
				}
			}
			if !anyMatches {
				unexpected = append(unexpected, actual)
			}
		case <-time.After(time.Second):
			break
		}
	}
	return expected, unexpected, all
}

func sendUpdates(t *testing.T, nc *nats.Conn, updates []*cvmsgspb.CronScriptUpdate) {
	for _, update := range updates {
		updateMsg, err := types.MarshalAny(update)
		require.NoError(t, err)
		c2vMsg := cvmsgspb.C2VMessage{Msg: updateMsg}
		data, err := c2vMsg.Marshal()
		require.NoError(t, err)
		require.NoError(t, nc.Publish(CronScriptUpdatesChannel, data))
	}
}

type stubCronScriptStore struct {
	GetScriptsResponse *metadatapb.GetScriptsResponse
	GetScriptsError    error

	AddOrUpdateScriptResponse *metadatapb.AddOrUpdateScriptResponse
	AddOrUpdateScriptError    error

	DeleteScriptResponse *metadatapb.DeleteScriptResponse
	DeleteScriptError    error

	SetScriptsResponse *metadatapb.SetScriptsResponse
	SetScriptsError    error

	RecordExecutionResultResponse *metadatapb.RecordExecutionResultResponse
	RecordExecutionResultError    error

	GetAllExecutionResultsResponse *metadatapb.GetAllExecutionResultsResponse
	GetAllExecutionResultsError    error
}

func (s *stubCronScriptStore) GetScripts(_ context.Context, _ *metadatapb.GetScriptsRequest, _ ...grpc.CallOption) (*metadatapb.GetScriptsResponse, error) {
	return s.GetScriptsResponse, s.GetScriptsError
}

func (s *stubCronScriptStore) AddOrUpdateScript(_ context.Context, _ *metadatapb.AddOrUpdateScriptRequest, _ ...grpc.CallOption) (*metadatapb.AddOrUpdateScriptResponse, error) {
	return s.AddOrUpdateScriptResponse, s.AddOrUpdateScriptError
}

func (s *stubCronScriptStore) DeleteScript(_ context.Context, _ *metadatapb.DeleteScriptRequest, _ ...grpc.CallOption) (*metadatapb.DeleteScriptResponse, error) {
	return s.DeleteScriptResponse, s.DeleteScriptError
}

func (s *stubCronScriptStore) SetScripts(_ context.Context, _ *metadatapb.SetScriptsRequest, _ ...grpc.CallOption) (*metadatapb.SetScriptsResponse, error) {
	return s.SetScriptsResponse, s.SetScriptsError
}

func (s *stubCronScriptStore) RecordExecutionResult(_ context.Context, _ *metadatapb.RecordExecutionResultRequest, _ ...grpc.CallOption) (*metadatapb.RecordExecutionResultResponse, error) {
	return s.RecordExecutionResultResponse, s.RecordExecutionResultError
}

func (s *stubCronScriptStore) GetAllExecutionResults(_ context.Context, _ *metadatapb.GetAllExecutionResultsRequest, _ ...grpc.CallOption) (*metadatapb.GetAllExecutionResultsResponse, error) {
	return s.GetAllExecutionResultsResponse, s.GetAllExecutionResultsError
}

func setupChecksumSubscription(t *testing.T, nc *nats.Conn, cloudScripts map[string]*cvmsgspb.CronScript) (*nats.Subscription, chan struct{}) {
	gotChecksumReq := make(chan struct{}, 1)
	checksumSub, err := nc.Subscribe(CronScriptChecksumRequestChannel, func(msg *nats.Msg) {
		v2cMsg := &cvmsgspb.V2CMessage{}
		err := proto.Unmarshal(msg.Data, v2cMsg)
		require.NoError(t, err)
		req := &cvmsgspb.GetCronScriptsChecksumRequest{}
		err = types.UnmarshalAny(v2cMsg.Msg, req)
		require.NoError(t, err)
		topic := req.Topic

		checksum, err := scripts.ChecksumFromScriptMap(cloudScripts)
		require.NoError(t, err)

		resp := &cvmsgspb.GetCronScriptsChecksumResponse{
			Checksum: checksum,
		}
		respMsg, err := types.MarshalAny(resp)
		require.NoError(t, err)
		c2vMsg := cvmsgspb.C2VMessage{
			Msg: respMsg,
		}
		data, err := c2vMsg.Marshal()
		require.NoError(t, err)

		err = nc.Publish(fmt.Sprintf("%s:%s", CronScriptChecksumResponseChannel, topic), data)
		require.NoError(t, err)
		gotChecksumReq <- struct{}{}
	})
	require.NoError(t, err)
	return checksumSub, gotChecksumReq
}

func setupCronScriptResponses(t *testing.T, nc *nats.Conn, updates []*cvmsgspb.CronScriptUpdate) (map[uuid.UUID]*nats.Subscription, map[uuid.UUID]chan struct{}) {
	subs := map[uuid.UUID]*nats.Subscription{}
	gotResponses := map[uuid.UUID]chan struct{}{}
	for _, update := range updates {
		func(update *cvmsgspb.CronScriptUpdate) {
			var err error
			var id uuid.UUID
			switch update.Msg.(type) {
			case *cvmsgspb.CronScriptUpdate_UpsertReq:
				id = utils.UUIDFromProtoOrNil(update.GetUpsertReq().Script.ID)
			case *cvmsgspb.CronScriptUpdate_DeleteReq:
				id = utils.UUIDFromProtoOrNil(update.GetDeleteReq().ScriptID)
			}
			gotResponses[id] = make(chan struct{}, 1024)
			subs[id], err = nc.Subscribe(fmt.Sprintf("%s:%s", CronScriptUpdatesResponseChannel, update.RequestID), func(msg *nats.Msg) {
				v2cMsg := &cvmsgspb.V2CMessage{}
				err := proto.Unmarshal(msg.Data, v2cMsg)
				require.NoError(t, err)
				switch update.Msg.(type) {
				case *cvmsgspb.CronScriptUpdate_UpsertReq:
					res := &cvmsgspb.RegisterOrUpdateCronScriptResponse{}
					err = types.UnmarshalAny(v2cMsg.Msg, res)
					require.NoError(t, err)
				case *cvmsgspb.CronScriptUpdate_DeleteReq:
					res := &cvmsgspb.DeleteCronScriptResponse{}
					err = types.UnmarshalAny(v2cMsg.Msg, res)
					require.NoError(t, err)
				default:
					t.Fatalf("unexpected conn script response %s", reflect.TypeOf(update.Msg).Name())
				}
				gotResponses[id] <- struct{}{}
			})
			require.NoError(t, err)
		}(update)
	}
	return subs, gotResponses
}

func setupCloudScriptsSubscription(t *testing.T, nc *nats.Conn, cloudScripts map[string]*cvmsgspb.CronScript) (*nats.Subscription, chan struct{}) {
	gotCronScripts := make(chan struct{}, 1)
	scriptSub, err := nc.Subscribe(GetCronScriptsRequestChannel, func(msg *nats.Msg) {
		v2cMsg := &cvmsgspb.V2CMessage{}
		err := proto.Unmarshal(msg.Data, v2cMsg)
		require.NoError(t, err)
		req := &cvmsgspb.GetCronScriptsRequest{}
		err = types.UnmarshalAny(v2cMsg.Msg, req)
		require.NoError(t, err)

		resp := &cvmsgspb.GetCronScriptsResponse{
			Scripts: cloudScripts,
		}
		respMsg, err := types.MarshalAny(resp)
		require.NoError(t, err)
		c2vMsg := cvmsgspb.C2VMessage{
			Msg: respMsg,
		}
		data, err := c2vMsg.Marshal()
		require.NoError(t, err)

		err = nc.Publish(fmt.Sprintf("%s:%s", GetCronScriptsResponseChannel, req.Topic), data)
		require.NoError(t, err)
		gotCronScripts <- struct{}{}
	})
	require.NoError(t, err)
	return scriptSub, gotCronScripts
}
