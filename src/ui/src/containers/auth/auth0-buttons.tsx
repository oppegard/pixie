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

import * as React from 'react';
import { GoogleButton, UsernamePasswordButton } from '@pixie-labs/components';

import { createStyles, Theme, makeStyles } from '@material-ui/core';

export interface Auth0ButtonsProps {
  action: 'Sign-Up' | 'Login';
  onGoogleButtonClick: () => void;
  onUsernamePasswordButtonClick: () => void;
}
const useStyles = makeStyles(({ spacing }: Theme) => createStyles({
  button: {
    width: '100%',
    marginTop: spacing(0.5),
    marginBottom: spacing(0.5),
  },
}));

export const Auth0Buttons: React.FC<Auth0ButtonsProps> = ({
  action,
  onGoogleButtonClick,
  onUsernamePasswordButtonClick,
}) => {
  const classes = useStyles();
  return (
    <>
      <div className={classes.button}>
        <UsernamePasswordButton
          text={`${action} with Password`}
          onClick={onUsernamePasswordButtonClick}
        />
      </div>
      <div className={classes.button}>
        <GoogleButton
          text={`${action} with Google`}
          onClick={onGoogleButtonClick}
        />
      </div>
    </>
  );
};
