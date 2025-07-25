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
						RgxGroupID: `^finops_.*$`,
					},
				},
			},
			args: args{
				consumerConfig: ConsumerConfig{
					GroupID: "test",
				},
			},
			want: ConsumerConfig{
				GroupID:    "finops_test",
				BatchCount: DefaultBatchCount,
				Skip: map[string]map[int32]OffsetConfig{
					"finops_serviceX_dlq": nil,
				},
				DLQ: DLQConfig{
					Topic:            "finops_serviceX_dlq",
					RetryInterval:    DefaultRetryInterval,
					RetryMaxInterval: DefaultRetryMaxInterval,
				},
				Concurrent: ConcurrentConfig{
					Enabled: false,
					MinSize: 1,
					Process: 10,
					RunSize: DefaultBatchCount,
					Type:    groupTypeKey.String(),
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := ConsumerPreConfig{
				PrefixGroupID:  tt.fields.PrefixGroupID,
				Validation:     tt.fields.Validation,
				FormatDLQTopic: "finops_{{.AppName}}_dlq",
			}
			err := configApply(c, &tt.args.consumerConfig, "serviceX", LogNoop{})
			if (err != nil) != tt.wantErr {
				t.Errorf("ConsumerPreConfig.Apply() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			got := tt.args.consumerConfig
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ConsumerPreConfig.Apply() = %v, want %v", got, tt.want)
			}
		})
	}
}
