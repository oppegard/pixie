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

import { gql, useQuery } from '@apollo/client';
import * as React from 'react';
import {
  Switch, Route, Redirect,
} from 'react-router-dom';
import * as QueryString from 'query-string';
import plHistory from 'app/utils/pl-history';
import { LocationDescriptorObject } from 'history';
import { SCRATCH_SCRIPT, ScriptsContext } from 'app/containers/App/scripts-context';
import { RouteNotFound } from 'app/containers/App/route-not-found';
import { selectClusterName } from 'app/containers/App/cluster-info';

import { GQLClusterInfo } from 'app/types/schema';
import { argsForVis } from 'app/utils/args-utils';

export interface LiveRouteContextProps {
  scriptId: string;
  widget: string | null;
  clusterName: string | null;
  args: Record<string, string | string[]>;
  push: (clusterName: string, scriptId: string, args: Record<string, string | string[]>) => void;
  replace: (clusterName: string, scriptId: string, args: Record<string, string | string[]>) => void;
  routeFor: (
    clusterName: string,
    scriptId: string,
    args: Record<string, string | string[]>,
  ) => LocationDescriptorObject;
}

export const LiveRouteContext = React.createContext<LiveRouteContextProps>(null);

/** Some scripts have special mnemonic routes. They are vanity URLs for /clusters/:cluster?... and map as such */
const VANITY_ROUTES = new Map<string, string>([
  /* eslint-disable no-multi-spaces */
  ['/live/clusters/:cluster',                                         'px/cluster'],
  ['/live/clusters/:cluster/nodes',                                   'px/nodes'],
  ['/live/clusters/:cluster/nodes/:node',                             'px/node'],
  ['/live/clusters/:cluster/namespaces',                              'px/namespaces'],
  ['/live/clusters/:cluster/namespaces/:namespace',                   'px/namespace'],
  ['/live/clusters/:cluster/namespaces/:namespace/pods',              'px/pods'],
  ['/live/clusters/:cluster/namespaces/:namespace/pods/:pod',         'px/pod'],
  ['/live/clusters/:cluster/namespaces/:namespace/services',          'px/services'],
  ['/live/clusters/:cluster/namespaces/:namespace/services/:service', 'px/service'],
  ['/live/clusters/:cluster/scratch',                                 SCRATCH_SCRIPT.id],
  // The bare live path will redirect to px/cluster but only if we have a clustername available to pick.
  ['/live',                                                           'px/cluster'],
  /* eslint-enable no-multi-spaces */
]);

const LiveRoute: React.FC<LiveRouteContextProps> = ({
  args, scriptId, widget, clusterName, push, replace, routeFor, children,
}) => {
  // Sorting keys ensures that the stringified object looks the same regardless of the order of operations that built it
  const serializedArgs = JSON.stringify(args, Object.keys(args ?? {}).sort());
  const context: LiveRouteContextProps = React.useMemo(() => ({
    scriptId, clusterName, widget, args, push, replace, routeFor,
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }), [scriptId, clusterName, widget, serializedArgs, push, replace, routeFor]);

  return (
    <LiveRouteContext.Provider value={context}>{children}</LiveRouteContext.Provider>
  );
};

const routeFor = (
  clusterName: string,
  scriptId: string,
  args: Record<string, string>,
): LocationDescriptorObject => {
  const route = `/live/clusters/${encodeURIComponent(clusterName)}`;
  const queryParams: Record<string, string> = {
    ...args,
    ...{ script: scriptId },
  };
  return { pathname: route, search: `?${QueryString.stringify(queryParams)}` };
};

const push = (
  clusterName: string,
  scriptId: string,
  args: Record<string, string>,
) => {
  const route = routeFor(clusterName, scriptId, args);
  if (
    route.pathname !== plHistory.location.pathname
    || route.search !== plHistory.location.search
  ) {
    plHistory.push(route);
  }
};

const replace = (
  clusterName: string,
  scriptId: string,
  args: Record<string, string>,
) => {
  const route = routeFor(clusterName, scriptId, args);
  if (
    route.pathname !== plHistory.location.pathname
    || route.search !== plHistory.location.search
  ) {
    plHistory.replace(route);
  }
};

export const LiveContextRouter: React.FC = ({ children }) => {
  const { data, loading: loadingCluster } = useQuery<{
    clusters: Pick<GQLClusterInfo, 'clusterName' | 'status'>[]
  }>(
    gql`
      query listClustersForLiveViewRouting {
        clusters {
          clusterName
          status
        }
      }
    `,
    { pollInterval: 15000 },
  );

  const clusters = data?.clusters;
  const defaultCluster = React.useMemo(() => selectClusterName(clusters ?? []), [clusters]);

  const { scripts: availableScripts, loading: loadingAvailableScripts } = React.useContext(ScriptsContext);

  if (loadingCluster || loadingAvailableScripts) return null; // Wait for things to be ready

  return (
    <Switch>
      <Route
        exact
        path={[...VANITY_ROUTES.keys()]}
        render={({ match, location }) => {
          // Special handling only if a default cluster is available and path is /live w/o args.
          // Otherwise we want to render the LiveRoute which eventually renders something helpful for new users.
          if (defaultCluster && match.path === '/live') {
            return (<Redirect to={`/live/clusters/${encodeURIComponent(defaultCluster)}`} />);
          }
          const { script: queryScriptId, widget: widgetName, ...queryParams } = QueryString.parse(location.search);
          let scriptId = VANITY_ROUTES.get(match.path) ?? 'px/cluster';
          if (queryScriptId) {
            scriptId = Array.isArray(queryScriptId)
              ? queryScriptId[0]
              : queryScriptId;
          }
          const { cluster, ...matchParams } = match.params;

          if (scriptId === 'px/pod' && matchParams.namespace != null && matchParams.pod != null) {
            matchParams.pod = `${matchParams.namespace}/${matchParams.pod}`;
            delete matchParams.namespace;
          }
          if (scriptId === 'px/service' && matchParams.namespace != null && matchParams.service != null) {
            matchParams.service = `${matchParams.namespace}/${matchParams.service}`;
            delete matchParams.namespace;
          }
          const args: Record<string, string | string[]> = argsForVis(
            availableScripts.get(scriptId)?.vis, {
              ...matchParams,
              ...queryParams,
            });

          return (
            <LiveRoute
              scriptId={scriptId}
              widget={Array.isArray(widgetName) ? widgetName[0] : widgetName}
              args={args}
              clusterName={decodeURIComponent(cluster)}
              push={push}
              replace={replace}
              routeFor={routeFor}
            >
              {children}
            </LiveRoute>
          );
        }}
      />
      <Route path='/live/*' component={RouteNotFound} />
    </Switch>
  );
};