// Copyright [2024] LinkedIn Corp. Licensed under the Apache License, Version
// 2.0 (the "License"); you may not use this file except in compliance with the
// License.  You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

package reconciliation_test

import (
	"io"

	"github.com/linkedin/goavro/v2"
)

// newOCFWriterForTest is a thin wrapper so tests do not depend on goavro
// internals.
func newOCFWriterForTest(w io.Writer, codec *goavro.Codec) (*goavro.OCFWriter, error) {
	return goavro.NewOCFWriter(goavro.OCFConfig{W: w, Codec: codec})
}
