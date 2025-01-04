package main

import (
	"context"
	"fmt"
	"io"
	"iter"
	"log"
	"os"
	"strings"

	mr "github.com/takanoriyanagitani/go-avro-maps2records"
	. "github.com/takanoriyanagitani/go-avro-maps2records/util"

	m2r "github.com/takanoriyanagitani/go-avro-maps2records/map2record"

	dh "github.com/takanoriyanagitani/go-avro-maps2records/avro/dec/hamba"
	eh "github.com/takanoriyanagitani/go-avro-maps2records/avro/enc/hamba"
	mh "github.com/takanoriyanagitani/go-avro-maps2records/map2record/hamba"
)

var EnvValByKey func(string) IO[string] = Lift(
	func(key string) (string, error) {
		val, found := os.LookupEnv(key)
		switch found {
		case true:
			return val, nil
		default:
			return "", fmt.Errorf("env var %s missing", key)
		}
	},
)

var stdin2maps IO[iter.Seq2[map[string]any, error]] = dh.
	StdinToMapsDefault

var schemaFilename IO[string] = EnvValByKey("ENV_SCHEMA_FILENAME")

func FilenameToStringLimited(limit int64) func(string) IO[string] {
	return Lift(func(filename string) (string, error) {
		f, e := os.Open(filename)
		if nil != e {
			return "", e
		}
		defer f.Close()

		limited := &io.LimitedReader{
			R: f,
			N: limit,
		}

		var buf strings.Builder
		_, e = io.Copy(&buf, limited)
		return buf.String(), e
	})
}

const SchemaFileSizeMax int64 = 1048576

var schemaContent IO[string] = Bind(
	schemaFilename,
	FilenameToStringLimited(SchemaFileSizeMax),
)

var simpleRecordInfo IO[mr.SimpleRecordInfo] = Bind(
	schemaContent,
	Lift(mh.SchemaToFields),
)

var map2record IO[m2r.MapToRecord] = Bind(
	simpleRecordInfo,
	Lift(func(s mr.SimpleRecordInfo) (m2r.MapToRecord, error) {
		return m2r.MapToRecordNew(s), nil
	}),
)

var mapd IO[iter.Seq2[map[string]any, error]] = Bind(
	map2record,
	func(m m2r.MapToRecord) IO[iter.Seq2[map[string]any, error]] {
		return Bind(
			stdin2maps,
			m.MapsToMaps,
		)
	},
)

var outputConfig IO[mr.OutputConfig] = Of(mr.OutputConfigDefault)

var encodeConfig IO[eh.EncodeConfig] = Bind(
	outputConfig,
	func(o mr.OutputConfig) IO[eh.EncodeConfig] {
		return Bind(
			schemaContent,
			Lift(func(s string) (eh.EncodeConfig, error) {
				return eh.EncodeConfig{
					Schema:       s,
					OutputConfig: o,
				}, nil
			}),
		)
	},
)

var stdin2avro2maps2mapd2avro2stdout IO[Void] = Bind(
	encodeConfig,
	func(cfg eh.EncodeConfig) IO[Void] {
		return Bind(
			mapd,
			eh.ConfigToMapsToStdout(cfg),
		)
	},
)

var sub IO[Void] = func(ctx context.Context) (Void, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	return stdin2avro2maps2mapd2avro2stdout(ctx)
}

func main() {
	_, e := sub(context.Background())
	if nil != e {
		log.Printf("%v\n", e)
	}
}
