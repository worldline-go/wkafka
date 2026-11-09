module github.com/worldline-go/wkafka/plugins/handler/redisauth/gcpiam

go 1.27

replace github.com/worldline-go/wkafka => ../../../../

require (
	cloud.google.com/go/auth v0.24.2
	github.com/worldline-go/wkafka v0.7.6
)

require (
	cloud.google.com/go/compute/metadata v0.10.0 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.23 // indirect
	github.com/googleapis/gax-go/v2 v2.27.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
)
