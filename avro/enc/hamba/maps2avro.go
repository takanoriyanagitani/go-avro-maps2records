package enc

import (
	"context"
	"io"
	"iter"
	"os"

	ha "github.com/hamba/avro/v2"
	ho "github.com/hamba/avro/v2/ocf"

	mr "github.com/takanoriyanagitani/go-avro-maps2records"
	. "github.com/takanoriyanagitani/go-avro-maps2records/util"
)

var codecMap map[mr.Codec]ho.CodecName = map[mr.Codec]ho.CodecName{
	mr.CodecNull:    ho.Null,
	mr.CodecDeflate: ho.Deflate,
	mr.CodecSnappy:  ho.Snappy,
	mr.CodecZstd:    ho.ZStandard,
}

func ConvertCodec(alt ho.CodecName, i mr.Codec) ho.CodecName {
	mapd, found := codecMap[i]
	switch found {
	case true:
		return mapd
	default:
		return alt
	}
}

var ConvertCodecDefault func(mr.Codec) ho.CodecName = Curry(
	ConvertCodec,
)(ho.Null)

func ConfigToFuncs(cfg mr.OutputConfig) []ho.EncoderFunc {
	var codec mr.Codec = cfg.Codec
	var mapd ho.CodecName = ConvertCodecDefault(codec)
	var blockLen int = cfg.BlockLength
	return []ho.EncoderFunc{
		ho.WithBlockLength(blockLen),
		ho.WithCodec(mapd),
	}
}

func MapsToWriterHamba(
	ctx context.Context,
	m iter.Seq2[map[string]any, error],
	w io.Writer,
	s ha.Schema,
	opts ...ho.EncoderFunc,
) error {
	enc, e := ho.NewEncoderWithSchema(s, w, opts...)
	if nil != e {
		return e
	}
	defer enc.Close()

	for row, e := range m {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if nil != e {
			return e
		}

		var ee error = enc.Encode(row)
		if nil != ee {
			return ee
		}

		var fe error = enc.Flush()
		if nil != fe {
			return fe
		}
	}
	return enc.Flush()
}

type EncodeConfig struct {
	Schema string
	mr.OutputConfig
}

func MapsToWriter(
	w io.Writer,
	cfg EncodeConfig,
) func(iter.Seq2[map[string]any, error]) IO[Void] {
	var opts []ho.EncoderFunc = ConfigToFuncs(cfg.OutputConfig)
	return func(m iter.Seq2[map[string]any, error]) IO[Void] {
		return func(ctx context.Context) (Void, error) {
			parsed, e := ha.Parse(cfg.Schema)
			if nil != e {
				return Empty, e
			}

			return Empty, MapsToWriterHamba(ctx, m, w, parsed, opts...)
		}
	}
}

func ConfigToMapsToStdout(
	cfg EncodeConfig,
) func(iter.Seq2[map[string]any, error]) IO[Void] {
	return MapsToWriter(os.Stdout, cfg)
}
