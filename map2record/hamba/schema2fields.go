package schema2fields

import (
	"errors"

	ha "github.com/hamba/avro/v2"

	mr "github.com/takanoriyanagitani/go-avro-maps2records"
	. "github.com/takanoriyanagitani/go-avro-maps2records/util"
)

var (
	ErrInvalidSchema error = errors.New("invalid schema")
)

func FieldsSchemaToFields(fields []*ha.Field) (mr.SimpleRecordInfo, error) {
	ret := mr.SimpleRecordInfo{
		Fields: make([]mr.SimpleFieldInfo, 0, len(fields)),
	}

	for _, field := range fields {
		ret.Fields = append(ret.Fields, mr.SimpleFieldInfo{
			Name: field.Name(),
		})
	}

	return ret, nil
}

func RecordSchemaToFields(rs *ha.RecordSchema) (mr.SimpleRecordInfo, error) {
	return FieldsSchemaToFields(rs.Fields())
}

func SchemaToFieldsHamba(s ha.Schema) (mr.SimpleRecordInfo, error) {
	switch t := s.(type) {
	case *ha.RecordSchema:
		return RecordSchemaToFields(t)
	default:
		return mr.SimpleRecordInfo{}, ErrInvalidSchema
	}
}

var SchemaToFields func(
	schema string,
) (mr.SimpleRecordInfo, error) = ComposeErr(
	ha.Parse,
	SchemaToFieldsHamba,
)
