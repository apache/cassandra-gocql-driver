/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gocql

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLimitedLogger(t *testing.T) {
	t.Run("captures the first message", func(t *testing.T) {
		logger := newTestLogger(LogLevelDebug)
		rateLimitedLogger := newLimitedLogger(10, logger)
		rateLimitedLogger.Debug("test message")

		log := logger.capture.String()
		assert.Equal(t, 1, strings.Count(log, "test message"))
	})

	t.Run("captures the first and every 10th message", func(t *testing.T) {
		logger := newTestLogger(LogLevelDebug)
		rateLimitedLogger := newLimitedLogger(10, logger)
		for i := 0; i < 100; i++ {
			rateLimitedLogger.Debug("test message")
		}

		log := logger.capture.String()
		assert.Equal(t, 10, strings.Count(log, "test message"))
	})

	t.Run("captures the first and every 1000th message", func(t *testing.T) {
		logger := newTestLogger(LogLevelDebug)
		rateLimitedLogger := newLimitedLogger(1000, logger)
		for i := 0; i < 10000; i++ {
			rateLimitedLogger.Debug("test message")
		}

		log := logger.capture.String()
		assert.Equal(t, 10, strings.Count(log, "test message"))
	})
}
