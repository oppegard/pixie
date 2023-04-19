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
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"google.golang.org/grpc/status"
	"px.dev/pixie/src/api/proto/uuidpb"
	"px.dev/pixie/src/carnot/planner/compilerpb"
	"px.dev/pixie/src/common/base/statuspb"

	"github.com/gofrs/uuid"
	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"gopkg.in/yaml.v2"

	"px.dev/pixie/src/api/proto/vizierpb"
	"px.dev/pixie/src/shared/cvmsgs"
	"px.dev/pixie/src/shared/cvmsgspb"
	"px.dev/pixie/src/shared/scripts"
	svcutils "px.dev/pixie/src/shared/services/utils"
	"px.dev/pixie/src/utils"
	"px.dev/pixie/src/vizier/services/metadata/metadatapb"
	"px.dev/pixie/src/vizier/utils/messagebus"
)

var (
	// CronScriptChecksumRequestChannel is the NATS channel to make checksum requests to.
	CronScriptChecksumRequestChannel = messagebus.V2CTopic(cvmsgs.CronScriptChecksumRequestChannel)
	// CronScriptChecksumResponseChannel is the NATS channel that checksum responses are published to.
	CronScriptChecksumResponseChannel = messagebus.C2VTopic(cvmsgs.CronScriptChecksumResponseChannel)
	// GetCronScriptsRequestChannel is the NATS channel script requests are sent to.
	GetCronScriptsRequestChannel = messagebus.V2CTopic(cvmsgs.GetCronScriptsRequestChannel)
	// GetCronScriptsResponseChannel is the NATS channel that script responses are published to.
	GetCronScriptsResponseChannel = messagebus.C2VTopic(cvmsgs.GetCronScriptsResponseChannel)
	// CronScriptUpdatesChannel is the NATS channel that any cron script updates are published to.
	CronScriptUpdatesChannel = messagebus.C2VTopic(cvmsgs.CronScriptUpdatesChannel)
	// CronScriptUpdatesResponseChannel is the NATS channel that script updates are published to.
	CronScriptUpdatesResponseChannel = messagebus.V2CTopic(cvmsgs.CronScriptUpdatesResponseChannel)
	natsWaitTimeout                  = 2 * time.Minute
	defaultOTelTimeoutS              = int64(5)
)

// ScriptRunner tracks registered cron scripts and runs them according to schedule.
type ScriptRunner struct {
	nc         *nats.Conn
	csClient   metadatapb.CronScriptStoreServiceClient
	vzClient   vizierpb.VizierServiceClient
	signingKey string

	runnerMap   map[uuid.UUID]*runner
	runnerMapMu sync.Mutex
	// scriptLastUpdateTime tracks the last time we latest update we processed for a script.
	// As we may receive updates out-of-order, this prevents us from processing a change out-of-order.
	scriptLastUpdateTime map[uuid.UUID]int64
	updateTimeMu         sync.Mutex

	done chan struct{}
	once sync.Once

	updatesCh  chan *nats.Msg
	updatesSub *nats.Subscription
}

// New creates a new script runner.
func New(nc *nats.Conn, csClient metadatapb.CronScriptStoreServiceClient, vzClient vizierpb.VizierServiceClient, signingKey string) (*ScriptRunner, error) {
	updatesCh := make(chan *nats.Msg, 4096)
	sub, err := nc.ChanSubscribe(CronScriptUpdatesChannel, updatesCh)
	if err != nil {
		log.WithError(err).Error("Failed to listen for cron script updates")
		return nil, err
	}

	sr := &ScriptRunner{nc: nc, csClient: csClient, done: make(chan struct{}), updatesCh: updatesCh, updatesSub: sub, scriptLastUpdateTime: make(map[uuid.UUID]int64), runnerMap: make(map[uuid.UUID]*runner), vzClient: vzClient, signingKey: signingKey}
	return sr, nil
}

// Stop performs any necessary cleanup before shutdown.
func (s *ScriptRunner) Stop() {
	s.once.Do(func() {
		close(s.updatesCh)
		s.updatesSub.Unsubscribe()
		close(s.done)
	})
}

// SyncScripts syncs the known set of scripts in Vizier with scripts in Cloud.
func (s *ScriptRunner) SyncScripts() error {
	// // Fetch persisted scripts.
	// claims := svcutils.GenerateJWTForService("cron_script_store", "vizier")
	// token, _ := svcutils.SignJWTClaims(claims, s.signingKey)
	//
	// ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization",
	// 	fmt.Sprintf("bearer %s", token))
	//
	// resp, err := s.csClient.GetScripts(ctx, &metadatapb.GetScriptsRequest{})
	// if err != nil {
	// 	log.WithError(err).Error("Failed to fetch scripts from store")
	// 	return err
	// }
	// scripts := resp.Scripts
	//
	// // Check if persisted scripts are up-to-date.
	// upToDate, err := s.compareScriptState(scripts)
	// if err != nil {
	// 	// In the case there is a failure, we should just refetch the scripts.
	// 	log.WithError(err).Error("Failed to verify script checksum")
	// }
	//
	// // TODO check for new configmaps or changed ones
	//
	// // If hash is not equal, fetch scripts from cloud.
	// if !upToDate {
	// 	cloudScripts, err := s.getCloudScripts()
	// 	if err != nil {
	// 		log.WithError(err).Error("Failed to fetch scripts from cloud")
	// 	} else {
	// 		// Clear out persisted scripts.
	// 		_, err = s.csClient.SetScripts(ctx, &metadatapb.SetScriptsRequest{Scripts: make(map[string]*cvmsgspb.CronScript)})
	// 		if err != nil {
	// 			log.WithError(err).Error("Failed to delete scripts from store")
	// 			return err
	// 		}
	// 		scripts = cloudScripts
	// 	}
	// }

	scriptUUID := uuid.FromStringOrNil("8fe72726-df03-11ed-b5ea-0242ac120002")
	scripts := map[string]*cvmsgspb.CronScript{
		scriptUUID.String(): {
			ID: &uuidpb.UUID{
				HighBits: binary.BigEndian.Uint64(scriptUUID[:8]),
				LowBits:  binary.BigEndian.Uint64(scriptUUID[8:]),
			},
			Script: `
# px:set max_output_rows_per_table=1500
import px


def exclude_known_namespaces(df):
    '''Exclude namespaces which are known to be noise.

    Args:
      @df: The DataFrames to filter.
    '''

    # Exclude DataFrames from kube-system
    #
    # It is not useful to see metrics related to the functioning of Kubernete's system-components (such as the CNI,
    # core-dns, aws-node, etc)
    df = df[df.ctx['namespace'] != 'kube-system']
    df = df[df.ctx['namespace'] != 'observability-system']
    df = df[df.ctx['namespace'] != 'olm']
    df = df[df.ctx['namespace'] != 'pl']
    df = df[df.ctx['namespace'] != 'px-operator']

    # Exclude DataFrames from aria-k8s
    #
    # This namespace is where the k8s-collector and telegraf-collector live. Excluding this namespace ensures that data
    # sent to Lemans from telegraf will not be re-reported by Pixie (since sending to Lemans happens over HTTP(s)
    df = df[df.ctx['namespace'] != 'aria-k8s']

    return df


def exclude_known_req_path(df):
    '''Exclude req_path values which are known to be noise.

    Args:
      @df: The DataFrames to filter.
    '''
    # Exclude DataFrames from liveness checks
    #
    # This endpoint is common in Kubernetes pods for checking liveness of a Pod.
    df = df[df.req_path != '/healthz']
    df = df[df.req_path != '/health']

    # Exclude DataFrames from readiness checks
    #
    # This endpoint is common in Kubernetes pods for checking readiness of a Pod.
    df = df[df.req_path != '/readyz']
    df = df[df.req_path != '/ready']

    return df

def exclude_egress_traffic(df):
    '''Exclude egress traffic i.e with trace_role=1.

    Args:
      @df: The DataFrames to filter.
    '''
    df = df[df['trace_role'] != 1]
    return df

def exclude_ingress_traffic(df):
    '''Exclude ingress traffic i.e we don't expect remote_addr to resolve to a pod_id and hence it would be empty)

    Args:
      @df: The DataFrames to filter.
    '''
    df.pod = df.ctx['pod']
    df.pod_id = df.ctx['pod_id']
    df.node = df.ctx['node']

    # If remote_addr doesn't correspond to a pod, then it returns an empty string per
    # https://docs.px.dev/reference/pxl/udf/ip_to_pod_id/#name
    df = df[px.ip_to_pod_id(df.remote_addr) != '']

    return df

def remove_ns_prefix(column):
    return px.replace('[a-z0-9\-]*/', column, '')


def parse_host_name(host_header):
    index = px.find(host_header, ':')
    return px.select(index > -1, px.substring(host_header, 0, index), host_header)

def xor(a, b):
    return a + b - 2 * a * b

def add_trace_headers(df):
    df.span_id = px.substring(df.destination_pod_id, 19, 4) + px.substring(df.destination_pod_id, 24, 12)
    df.parent_span_id = px.substring(df.source_pod_id, 19, 4) + px.substring(df.source_pod_id, 24, 12)

    # If we have 16 decimal places of nanoseconds, then we will potentially have collisions about every 115 days.
    # Because of that, we are xor'ing the time and latency with the intention of generating a more unique trace id.
    trace_id_time_part = "0000000000000000" + px.itoa(xor(px.time_to_int64(df.time_), df.latency))
    df.trace_id = px.substring(df.span_id, 8, 8) + px.substring(df.parent_span_id, 8, 8) + px.substring(trace_id_time_part, px.length(trace_id_time_part)-16, 16)
    return df

def default_none(value):
    return px.select(value != '', value, 'none')

# Assumes only server side http events (i.e. trace_role=2)
def add_source_dest_columns(df):
    df.destination_pod_id = df.pod_id
    df.destination_pod = remove_ns_prefix(df.pod)
    df.destination_deployment = remove_ns_prefix(default_none(px.pod_id_to_deployment_name(df.destination_pod_id)))
    df.destination_service = remove_ns_prefix(default_none(px.pod_id_to_service_name(df.destination_pod_id)))
    df.destination_namespace = px.pod_id_to_namespace(df.destination_pod_id)
    df.destination_node = df.node

    # After excluding ingress and egress traffic, remote_addr is guaranteed to be a pod. So pod_id can't be empty.
    df.source_pod_id = px.ip_to_pod_id(df.remote_addr)
    df.source_pod = remove_ns_prefix(px.pod_id_to_pod_name(df.source_pod_id))
    df.source_deployment = remove_ns_prefix(default_none(px.pod_id_to_deployment_name(df.source_pod_id)))

    # order of precedence: service = service_name > deployment_name > unknown_ingress
    df.source_service = remove_ns_prefix(px.pod_id_to_service_name(df.source_pod_id))
    df.source_service = px.select(df.source_service == '', df.source_deployment, df.source_service)
    df.source_service = px.select(df.source_service == '', 'unknown_ingress', df.source_service)
    df.source_node = px.pod_id_to_node_name(df.source_pod_id)
    df.source_namespace = px.pod_id_to_namespace(df.source_pod_id)

    return df

def add_error_tag(df):
    # We are following pixie's error convention that does not handle errors in 3xx and network errors in response body
    # as defined in the otel spec below:
    # https://opentelemetry.io/docs/reference/specification/trace/semantic_conventions/http/#status
    # Pixie reference:
    # https://github.com/pixie-io/pixie/blob/ce44943ecc3dc2f029464927534786d968b97558/src/pxl_scripts/px/namespace/namespace.pxl#L200
    df.error = px.select((df.resp_status >= 400), "true", "false")

    return df

df = px.DataFrame('http_events', start_time=px.plugin.start_time, end_time=px.plugin.end_time)

df = exclude_known_namespaces(df)
df = exclude_known_req_path(df)
df = exclude_egress_traffic(df)
df = exclude_ingress_traffic(df)
df = add_source_dest_columns(df)
df = add_trace_headers(df)
df = add_error_tag(df)
df = df.head(15000)
df.start_time = df.time_ - df.latency

host_header = px.pluck(df.req_headers, 'Host')
host_header = px.select(host_header != '', host_header, 'none')
df.host_name = parse_host_name(host_header)

scheme = px.select(df.remote_port == 443, "https://", "http://")
df.req_url = scheme + host_header + df.req_path

df.user_agent = px.pluck(df.req_headers, 'User-Agent')
df.user_agent = px.select(df.user_agent != '', df.user_agent, 'none')

# Strip out all but the actual path value from req_path
df.req_path = px.uri_recompose('', '', '', 0, px.pluck(px.uri_parse(df.req_path), 'path'), '', '')
# Replace any Hex IDS from the path with <id>
df.req_path = px.replace('/[a-fA-F0-9\-:]{6,}(/?)', df.req_path, '/<id>\\1')
df.cluster_name = px.vizier_name()

px.export(
    df, px.otel.Data(
        resource={
            'service.name': df.destination_service,
            'service.instance.id': df.destination_pod,

            'k8s.cluster.name': df.cluster_name,

            # OTEL Scope Exporter Conventions
            'otel.scope.name': 'pixie',

        },
        data=[
            px.otel.trace.Span(
                name=df.req_path,
                start_time=df.start_time,
                end_time=df.time_,
                trace_id=df.trace_id,
                span_id=df.span_id,
                parent_span_id=df.parent_span_id,
                kind=px.otel.trace.SPAN_KIND_SERVER,
                attributes={
                    # NOTE: the integration handles splitting of services.

                    # OTEL K8s Resource Conventions
                    'k8s.pod.name': df.destination_pod,
                    'k8s.node.name': df.destination_node,
                    'k8s.deployment.name': df.destination_deployment,
                    'k8s.namespace.name': df.destination_namespace,

                    # Wavefront Conventions
                    'service': df.destination_service,
                    'source': df.destination_pod,
                    'application': df.destination_namespace,
                    # We follow pixie's convention of setting an error tag only for a server span.
                    'error': df.error,

                    # OTEL HTTP Conventions
                    'http.status_code': df.resp_status,
                    'http.request_content_length': df.req_body_size,
                    'http.response_content_length': df.req_body_size,
                    'http.method': df.req_method,
                    'http.url': df.req_url,
                    'http.target': df.req_path,
                    'net.host.name': df.host_name,
                    'net.host.port': df.remote_port,
                    'user_agent.original': df.user_agent,
                },
            ),
            px.otel.trace.Span(
                name=df.req_path,
                start_time=df.start_time,
                end_time=df.time_,
                trace_id=df.trace_id,
                span_id=df.parent_span_id,
                kind=px.otel.trace.SPAN_KIND_CLIENT,
                attributes={
                    # NOTE: the integration handles splitting of services.

                    # OTEL K8s Resource Conventions
                    'k8s.pod.name': df.source_pod,
                    'k8s.node.name': df.source_node,
                    'k8s.deployment.name': df.source_deployment,
                    'k8s.namespace.name': df.source_namespace,

                    # Wavefront Conventions
                    'service': df.source_service,
                    'source': df.source_pod,
                    'application': df.source_namespace,

                    # OTEL HTTP Conventions
                    'http.status_code': df.resp_status,
                    'http.request_content_length': df.req_body_size,
                    'http.response_content_length': df.req_body_size,
                    'http.method': df.req_method,
                    'http.url': df.req_url,
                    'http.target': df.req_path,
                    'net.host.name': df.host_name,
                    'net.host.port': df.remote_port,
                    'user_agent.original': df.user_agent,
                },
            ),
        ],
    ),
)
`,
			Configs: `
otelEndpointConfig:
	url: "wavefront-proxy.observability-system.svc.cluster.local:4317"
`,
			FrequencyS: 60,
		},
	}

	// Add runners.
	for k, v := range scripts {
		err := s.upsertScript(uuid.FromStringOrNil(k), v)
		if err != nil {
			log.WithError(err).Error("Failed to upsert script, skipping...")
		}
	}
	return nil
}

func (s *ScriptRunner) processUpdates() {
	for {
		select {
		case <-s.done:
			return
		case msg := <-s.updatesCh:
			c2vMsg := &cvmsgspb.C2VMessage{}
			err := proto.Unmarshal(msg.Data, c2vMsg)
			if err != nil {
				log.WithError(err).Error("Failed to unmarshal c2v message")
				continue
			}
			resp := &cvmsgspb.CronScriptUpdate{}
			err = types.UnmarshalAny(c2vMsg.Msg, resp)
			if err != nil {
				log.WithError(err).Error("Failed to unmarshal c2v message")
				continue
			}

			switch resp.Msg.(type) {
			case *cvmsgspb.CronScriptUpdate_UpsertReq:
				uResp := resp.GetUpsertReq()

				// Filter out out-of-order updates.
				sID := utils.UUIDFromProtoOrNil(uResp.Script.ID)
				time := int64(0)
				s.updateTimeMu.Lock()
				if v, ok := s.scriptLastUpdateTime[sID]; ok {
					time = v
				}

				if time < resp.Timestamp {
					err := s.upsertScript(sID, uResp.Script)
					if err != nil {
						log.WithError(err).Error("Failed to upsert script")
					}
					s.scriptLastUpdateTime[sID] = resp.Timestamp
				}
				s.updateTimeMu.Unlock()

				// Send response.
				r := &cvmsgspb.RegisterOrUpdateCronScriptResponse{}
				reqAnyMsg, err := types.MarshalAny(r)
				if err != nil {
					log.WithError(err).Error("Failed to marshal update script response")
					continue
				}
				v2cMsg := cvmsgspb.V2CMessage{
					Msg: reqAnyMsg,
				}
				// Publish request.
				b, err := v2cMsg.Marshal()
				if err != nil {
					log.WithError(err).Error("Failed to marshal update script response")
					continue
				}
				err = s.nc.Publish(fmt.Sprintf("%s:%s", CronScriptUpdatesResponseChannel, resp.RequestID), b)
				if err != nil {
					log.WithError(err).Error("Failed to publish update script response")
				}
			case *cvmsgspb.CronScriptUpdate_DeleteReq:
				dResp := resp.GetDeleteReq()

				// Filter out out-of-order updates.
				sID := utils.UUIDFromProtoOrNil(dResp.ScriptID)
				time := int64(0)
				s.updateTimeMu.Lock()
				if v, ok := s.scriptLastUpdateTime[sID]; ok {
					time = v
				}

				if time < resp.Timestamp { // Update is newer than last processed update.
					err := s.deleteScript(sID)
					if err != nil {
						log.WithError(err).Error("Failed to delete script")
					}
					s.scriptLastUpdateTime[sID] = resp.Timestamp
				}
				s.updateTimeMu.Unlock()

				// Send response.
				r := &cvmsgspb.DeleteCronScriptResponse{}
				reqAnyMsg, err := types.MarshalAny(r)
				if err != nil {
					log.WithError(err).Error("Failed to marshal update script response")
					continue
				}
				v2cMsg := cvmsgspb.V2CMessage{
					Msg: reqAnyMsg,
				}
				// Publish request.
				b, err := v2cMsg.Marshal()
				if err != nil {
					log.WithError(err).Error("Failed to marshal update script response")
					continue
				}
				err = s.nc.Publish(fmt.Sprintf("%s:%s", CronScriptUpdatesResponseChannel, resp.RequestID), b)
				if err != nil {
					log.WithError(err).Error("Failed to publish update script response")
				}
			default:
				log.Error("Received unknown message for cronScriptUpdate")
			}
		}
	}
}

func (s *ScriptRunner) upsertScript(id uuid.UUID, script *cvmsgspb.CronScript) error {
	s.runnerMapMu.Lock()
	defer s.runnerMapMu.Unlock()

	if v, ok := s.runnerMap[id]; ok {
		v.stop()
		delete(s.runnerMap, id)
	}
	r := newRunner(script, s.vzClient, s.signingKey, id, s.csClient)
	s.runnerMap[id] = r
	go r.start()
	claims := svcutils.GenerateJWTForService("cron_script_store", "vizier")
	token, _ := svcutils.SignJWTClaims(claims, s.signingKey)

	ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization",
		fmt.Sprintf("bearer %s", token))

	_, err := s.csClient.AddOrUpdateScript(ctx, &metadatapb.AddOrUpdateScriptRequest{Script: script})
	if err != nil {
		log.WithError(err).Error("Failed to upsert script in metadata")
	}

	return nil
}

func (s *ScriptRunner) deleteScript(id uuid.UUID) error {
	s.runnerMapMu.Lock()
	defer s.runnerMapMu.Unlock()

	v, ok := s.runnerMap[id]
	if !ok {
		return nil
	}
	v.stop()
	delete(s.runnerMap, id)
	claims := svcutils.GenerateJWTForService("cron_script_store", "vizier")
	token, _ := svcutils.SignJWTClaims(claims, s.signingKey)

	ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization",
		fmt.Sprintf("bearer %s", token))

	_, err := s.csClient.DeleteScript(ctx, &metadatapb.DeleteScriptRequest{ScriptID: utils.ProtoFromUUID(id)})
	if err != nil {
		log.WithError(err).Error("Failed to delete script from metadata")
	}

	return nil
}

func (s *ScriptRunner) compareScriptState(existingScripts map[string]*cvmsgspb.CronScript) (bool, error) {
	// Get hash of map.
	existingChecksum, err := scripts.ChecksumFromScriptMap(existingScripts)
	if err != nil {
		return false, err
	}

	topicID := uuid.Must(uuid.NewV4())
	req := &cvmsgspb.GetCronScriptsChecksumRequest{
		Topic: topicID.String(),
	}
	reqAnyMsg, err := types.MarshalAny(req)
	if err != nil {
		return false, err
	}
	v2cMsg := cvmsgspb.V2CMessage{
		Msg: reqAnyMsg,
	}
	c2vMsg, err := s.natsReplyAndResponse(&v2cMsg, CronScriptChecksumRequestChannel, fmt.Sprintf("%s:%s", CronScriptChecksumResponseChannel, topicID.String()))
	if err != nil {
		return false, err
	}

	resp := &cvmsgspb.GetCronScriptsChecksumResponse{}
	err = types.UnmarshalAny(c2vMsg.Msg, resp)
	if err != nil {
		log.WithError(err).Error("Failed to unmarshal checksum response")
		return false, err
	}
	return existingChecksum == resp.Checksum, nil
}

func (s *ScriptRunner) getCloudScripts() (map[string]*cvmsgspb.CronScript, error) {
	topicID := uuid.Must(uuid.NewV4())
	req := &cvmsgspb.GetCronScriptsRequest{
		Topic: topicID.String(),
	}
	reqAnyMsg, err := types.MarshalAny(req)
	if err != nil {
		return nil, err
	}
	v2cMsg := cvmsgspb.V2CMessage{
		Msg: reqAnyMsg,
	}

	c2vMsg, err := s.natsReplyAndResponse(&v2cMsg, GetCronScriptsRequestChannel, fmt.Sprintf("%s:%s", GetCronScriptsResponseChannel, topicID.String()))
	if err != nil {
		return nil, err
	}

	resp := &cvmsgspb.GetCronScriptsResponse{}
	err = types.UnmarshalAny(c2vMsg.Msg, resp)
	if err != nil {
		log.WithError(err).Error("Failed to unmarshal checksum response")
		return nil, err
	}
	return resp.Scripts, nil
}

func (s *ScriptRunner) natsReplyAndResponse(req *cvmsgspb.V2CMessage, requestTopic string, responseTopic string) (*cvmsgspb.C2VMessage, error) {
	// Subscribe to topic that the response will be sent on.
	subCh := make(chan *nats.Msg, 4096)
	sub, err := s.nc.ChanSubscribe(responseTopic, subCh)
	if err != nil {
		return nil, err
	}
	defer sub.Unsubscribe()

	// Publish request.
	b, err := req.Marshal()
	if err != nil {
		return nil, err
	}

	err = s.nc.Publish(requestTopic, b)
	if err != nil {
		return nil, err
	}

	// Wait for response.
	t := time.NewTimer(natsWaitTimeout)
	defer t.Stop()
	for {
		select {
		case <-s.done:
			return nil, errors.New("Cancelled")
		case msg := <-subCh:
			c2vMsg := &cvmsgspb.C2VMessage{}
			err := proto.Unmarshal(msg.Data, c2vMsg)
			if err != nil {
				log.WithError(err).Error("Failed to unmarshal c2v message")
				return nil, err
			}
			return c2vMsg, nil
		case <-t.C:
			return nil, errors.New("Failed to get response")
		}
	}
}

// Logic for "runners" which handle the script execution.
type runner struct {
	cronScript *cvmsgspb.CronScript
	config     *scripts.Config

	lastRun time.Time

	csClient   metadatapb.CronScriptStoreServiceClient
	vzClient   vizierpb.VizierServiceClient
	signingKey string

	done chan struct{}
	once sync.Once

	scriptID uuid.UUID
}

func newRunner(script *cvmsgspb.CronScript, vzClient vizierpb.VizierServiceClient, signingKey string, id uuid.UUID, csClient metadatapb.CronScriptStoreServiceClient) *runner {
	// Parse config YAML into struct.
	var config scripts.Config
	err := yaml.Unmarshal([]byte(script.Configs), &config)
	if err != nil {
		log.WithError(err).Error("Failed to parse config YAML")
	}

	return &runner{
		cronScript: script, done: make(chan struct{}), csClient: csClient, vzClient: vzClient, signingKey: signingKey, config: &config, scriptID: id,
	}
}

// VizierStatusToStatus converts the Vizier status to the internal storable version statuspb.Status
func VizierStatusToStatus(s *vizierpb.Status) (*statuspb.Status, error) {
	var ctxAny *types.Any
	var err error
	if len(s.ErrorDetails) > 0 {
		errorPb := &compilerpb.CompilerErrorGroup{
			Errors: make([]*compilerpb.CompilerError, len(s.ErrorDetails)),
		}
		for i, ed := range s.ErrorDetails {
			e := ed.GetCompilerError()
			errorPb.Errors[i] = &compilerpb.CompilerError{
				Error: &compilerpb.CompilerError_LineColError{
					LineColError: &compilerpb.LineColError{
						Line:    e.Line,
						Column:  e.Column,
						Message: e.Message,
					},
				},
			}
		}
		ctxAny, err = types.MarshalAny(errorPb)
		if err != nil {
			return nil, err
		}
	}
	return &statuspb.Status{
		ErrCode: statuspb.Code(s.Code),
		Msg:     s.Message,
		Context: ctxAny,
	}, nil
}

func (r *runner) runScript(scriptPeriod time.Duration) {
	claims := svcutils.GenerateJWTForService("query_broker", "vizier")
	token, _ := svcutils.SignJWTClaims(claims, r.signingKey)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization",
		fmt.Sprintf("bearer %s", token))

	var otelEndpoint *vizierpb.Configs_OTelEndpointConfig
	if r.config != nil && r.config.OtelEndpointConfig != nil {
		otelEndpoint = &vizierpb.Configs_OTelEndpointConfig{
			URL:      r.config.OtelEndpointConfig.URL,
			Headers:  r.config.OtelEndpointConfig.Headers,
			Insecure: r.config.OtelEndpointConfig.Insecure,
			Timeout:  defaultOTelTimeoutS,
		}
	}

	// We set the time 1 second in the past to cover colletor latency and request latencies
	// which can cause data overlaps or cause data to be missed.
	startTime := r.lastRun.Add(-time.Second)
	endTime := startTime.Add(scriptPeriod)
	r.lastRun = time.Now()
	execScriptClient, err := r.vzClient.ExecuteScript(ctx, &vizierpb.ExecuteScriptRequest{
		QueryStr: r.cronScript.Script,
		Configs: &vizierpb.Configs{
			OTelEndpointConfig: otelEndpoint,
			PluginConfig: &vizierpb.Configs_PluginConfig{
				StartTimeNs: startTime.UnixNano(),
				EndTimeNs:   endTime.UnixNano(),
			},
		},
		QueryName: "cron_" + r.scriptID.String(),
	})
	if err != nil {
		log.WithError(err).Error("Failed to execute cronscript")
	}
	for {
		resp, err := execScriptClient.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			grpcStatus, _ := status.FromError(err)

			tsPb, err := types.TimestampProto(startTime)
			if err != nil {
				log.WithError(err).Error("Error while creating timestamp proto")
			}

			_, err = r.csClient.RecordExecutionResult(ctx, &metadatapb.RecordExecutionResultRequest{
				ScriptID:  utils.ProtoFromUUID(r.scriptID),
				Timestamp: tsPb,
				Result: &metadatapb.RecordExecutionResultRequest_Error{
					Error: &statuspb.Status{
						ErrCode: statuspb.Code(grpcStatus.Code()),
						Msg:     grpcStatus.Message(),
					},
				},
			})
			if err != nil {
				grpcStatus, ok := status.FromError(err)
				if !ok || grpcStatus.Code() != codes.Unavailable {
					log.WithError(err).Error("Error while recording cron script execution error")
				}
			}
			break
		}

		if vzStatus := resp.GetStatus(); vzStatus != nil {
			tsPb, err := types.TimestampProto(startTime)
			if err != nil {
				log.WithError(err).Error("Error while creating timestamp proto")
			}
			st, err := VizierStatusToStatus(vzStatus)
			if err != nil {
				log.WithError(err).Error("Error converting status")
			}

			_, err = r.csClient.RecordExecutionResult(ctx, &metadatapb.RecordExecutionResultRequest{
				ScriptID:  utils.ProtoFromUUID(r.scriptID),
				Timestamp: tsPb,
				Result: &metadatapb.RecordExecutionResultRequest_Error{
					Error: st,
				},
			})
			if err != nil {
				grpcStatus, ok := status.FromError(err)
				if !ok || grpcStatus.Code() != codes.Unavailable {
					log.WithError(err).Error("Error while recording cron script execution error")
				}
			}
			break
		}
		if data := resp.GetData(); data != nil {
			tsPb, err := types.TimestampProto(startTime)
			if err != nil {
				log.WithError(err).Error("Error while creating timestamp proto")
			}
			stats := data.GetExecutionStats()
			if stats == nil {
				continue
			}
			log.Infof("execution stats: id=%s execution_time_ns=%d compilation_time_ns=%d bytes_processed=%d records_processed=%d",
				r.scriptID.String(),
				stats.Timing.ExecutionTimeNs,
				stats.Timing.CompilationTimeNs,
				stats.BytesProcessed,
				stats.RecordsProcessed,
			)
			_, err = r.csClient.RecordExecutionResult(ctx, &metadatapb.RecordExecutionResultRequest{
				ScriptID:  utils.ProtoFromUUID(r.scriptID),
				Timestamp: tsPb,
				Result: &metadatapb.RecordExecutionResultRequest_ExecutionStats{
					ExecutionStats: &metadatapb.ExecutionStats{
						ExecutionTimeNs:   stats.Timing.ExecutionTimeNs,
						CompilationTimeNs: stats.Timing.CompilationTimeNs,
						BytesProcessed:    stats.BytesProcessed,
						RecordsProcessed:  stats.RecordsProcessed,
					},
				},
			})
			if err != nil {
				grpcStatus, ok := status.FromError(err)
				if !ok || grpcStatus.Code() != codes.Unavailable {
					log.WithError(err).Error("Error recording execution stats")
				}
			}
			break
		}
	}
}

func (r *runner) start() {
	if r.cronScript.FrequencyS <= 0 {
		return
	}
	scriptPeriod := time.Duration(r.cronScript.FrequencyS) * time.Second
	ticker := time.NewTicker(scriptPeriod)
	r.lastRun = time.Now()

	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-r.done:
				return
			case <-ticker.C:
				r.runScript(scriptPeriod)
			}
		}
	}()
}

func (r *runner) stop() {
	r.once.Do(func() {
		close(r.done)
	})
}
