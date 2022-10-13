// Copyright 2012-2018 The NATS Authors
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

package mongo

import (
	"github.com/nats-io/nats.go"
	"go.mongodb.org/mongo-driver/bson"
	"strings"
)

// Additional index for registered Encoders.
const (
	BSON_ECODER = "bson"
)

func init() {
	// Register bson encoder
	nats.RegisterEncoder(BSON_ECODER, &BsonEncoder{})
}

// BsonEncoder is a BSON Encoder implementation for EncodedConn.
// This encoder will use the mongo bson encoder to Marshal
// and Unmarshal most types, including structs.
type BsonEncoder struct{}

//Encode encode v to bson
func (be *BsonEncoder) Encode(subject string, v interface{}) ([]byte, error) {
	b, err := bson.Marshal(v)
	if err != nil {
		return nil, err
	}
	return b, nil
}

//Decode decode data to bson
func (be *BsonEncoder) Decode(subject string, data []byte, vPtr interface{}) (err error) {
	switch arg := vPtr.(type) {
	case *string:
		str := string(data)
		if strings.HasPrefix(str, `"`) && strings.HasSuffix(str, `"`) {
			*arg = str[1 : len(str)-1]
		} else {
			*arg = str
		}
	case *[]byte:
		*arg = data
	default:
		err = bson.Unmarshal(data, arg)
	}
	return
}
