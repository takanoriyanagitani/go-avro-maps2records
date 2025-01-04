package map2record

import (
	"context"
	"iter"

	mr "github.com/takanoriyanagitani/go-avro-maps2records"
	. "github.com/takanoriyanagitani/go-avro-maps2records/util"
)

type MapToRecord func(mr.GenericMap) IO[mr.Record]

func MapToRecordNew(
	recordInfo mr.SimpleRecordInfo,
) MapToRecord {
	buf := mr.Record{}
	return func(i mr.GenericMap) IO[mr.Record] {
		return func(_ context.Context) (mr.Record, error) {
			clear(buf)

			for _, field := range recordInfo.Fields {
				var name string = field.Name
				buf[name] = i[name]
			}

			return buf, nil
		}
	}
}

func (m MapToRecord) MapsToMaps(
	original iter.Seq2[map[string]any, error],
) IO[iter.Seq2[map[string]any, error]] {
	return func(ctx context.Context) (iter.Seq2[map[string]any, error], error) {
		return func(yield func(map[string]any, error) bool) {
			buf := map[string]any{}
			for row, e := range original {
				clear(buf)

				if nil != e {
					yield(buf, e)
					return
				}

				var gmap mr.GenericMap = row

				rec, e := m(gmap)(ctx)
				if !yield(rec, e) {
					return
				}
			}
		}, nil
	}
}
