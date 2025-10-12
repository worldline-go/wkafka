package wkafka

import "testing"

func Test_templateRun(t *testing.T) {
	type args struct {
		txt  string
		data any
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "test",
			args: args{
				txt:  "finops_{{.AppName}}_dlq",
				data: map[string]string{"AppName": "test"},
			},
			want: "finops_test_dlq",
		},
		{
			name: "test-2",
			args: args{
				txt:  "finops_dlq",
				data: map[string]string{"AppName": "test"},
			},
			want: "finops_dlq",
		},
		{
			name: "test error",
			args: args{
				txt:  "finops_{{.AppName}_dlq",
				data: nil,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := templateRun(tt.args.txt, tt.args.data)
			if (err != nil) != tt.wantErr {
				t.Errorf("templateRun() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("templateRun() = %v, want %v", got, tt.want)
			}
		})
	}
}
