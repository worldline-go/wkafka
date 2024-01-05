package wkafka

import (
	"reflect"
	"testing"
)

func TestConsumerPreConfig_Apply(t *testing.T) {
	type fields struct {
		PrefixGroupID string
		Validation    Validation
	}
	type args struct {
		consumerConfig ConsumerConfig
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    ConsumerConfig
		wantErr bool
	}{
		{
			name: "valid",
			fields: fields{
				PrefixGroupID: "finops_",
				Validation: Validation{
					GroupID: GroupIDValidation{
						Enabled:    true,
						RgxGroupID: `finops_.*`,
					},
				},
			},
			args: args{
				consumerConfig: ConsumerConfig{
					GroupID: "test",
				},
			},
			want: ConsumerConfig{
				GroupID: "finops_test",
				DLQ: DLQ{
					Topic: "finops_serviceX_dlq",
					SkipExtra: map[string]map[int32]Offsets{
						"finops_serviceX_dlq": nil,
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := ConsumerPreConfig{
				PrefixGroupID: tt.fields.PrefixGroupID,
				Validation:    tt.fields.Validation,
			}
			got, err := c.Apply(tt.args.consumerConfig, "serviceX")
			if (err != nil) != tt.wantErr {
				t.Errorf("ConsumerPreConfig.Apply() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ConsumerPreConfig.Apply() = %v, want %v", got, tt.want)
			}
		})
	}
}
