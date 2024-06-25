// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.34.2
// 	protoc        (unknown)
// source: wkafka/wkafka.proto

package wkafka

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type SkipOption int32

const (
	SkipOption_APPEND  SkipOption = 0
	SkipOption_REPLACE SkipOption = 1
)

// Enum value maps for SkipOption.
var (
	SkipOption_name = map[int32]string{
		0: "APPEND",
		1: "REPLACE",
	}
	SkipOption_value = map[string]int32{
		"APPEND":  0,
		"REPLACE": 1,
	}
)

func (x SkipOption) Enum() *SkipOption {
	p := new(SkipOption)
	*p = x
	return p
}

func (x SkipOption) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (SkipOption) Descriptor() protoreflect.EnumDescriptor {
	return file_wkafka_wkafka_proto_enumTypes[0].Descriptor()
}

func (SkipOption) Type() protoreflect.EnumType {
	return &file_wkafka_wkafka_proto_enumTypes[0]
}

func (x SkipOption) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use SkipOption.Descriptor instead.
func (SkipOption) EnumDescriptor() ([]byte, []int) {
	return file_wkafka_wkafka_proto_rawDescGZIP(), []int{0}
}

type CreateSkipRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Topics           map[string]*Topic `protobuf:"bytes,1,rep,name=topics,proto3" json:"topics,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
	Option           SkipOption        `protobuf:"varint,2,opt,name=option,proto3,enum=wkafka.SkipOption" json:"option,omitempty"`
	EnableMainTopics bool              `protobuf:"varint,3,opt,name=enable_main_topics,json=enableMainTopics,proto3" json:"enable_main_topics,omitempty"`
}

func (x *CreateSkipRequest) Reset() {
	*x = CreateSkipRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_wkafka_wkafka_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *CreateSkipRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CreateSkipRequest) ProtoMessage() {}

func (x *CreateSkipRequest) ProtoReflect() protoreflect.Message {
	mi := &file_wkafka_wkafka_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CreateSkipRequest.ProtoReflect.Descriptor instead.
func (*CreateSkipRequest) Descriptor() ([]byte, []int) {
	return file_wkafka_wkafka_proto_rawDescGZIP(), []int{0}
}

func (x *CreateSkipRequest) GetTopics() map[string]*Topic {
	if x != nil {
		return x.Topics
	}
	return nil
}

func (x *CreateSkipRequest) GetOption() SkipOption {
	if x != nil {
		return x.Option
	}
	return SkipOption_APPEND
}

func (x *CreateSkipRequest) GetEnableMainTopics() bool {
	if x != nil {
		return x.EnableMainTopics
	}
	return false
}

type Topic struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Partitions map[int32]*Partition `protobuf:"bytes,1,rep,name=partitions,proto3" json:"partitions,omitempty" protobuf_key:"varint,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
}

func (x *Topic) Reset() {
	*x = Topic{}
	if protoimpl.UnsafeEnabled {
		mi := &file_wkafka_wkafka_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Topic) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Topic) ProtoMessage() {}

func (x *Topic) ProtoReflect() protoreflect.Message {
	mi := &file_wkafka_wkafka_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Topic.ProtoReflect.Descriptor instead.
func (*Topic) Descriptor() ([]byte, []int) {
	return file_wkafka_wkafka_proto_rawDescGZIP(), []int{1}
}

func (x *Topic) GetPartitions() map[int32]*Partition {
	if x != nil {
		return x.Partitions
	}
	return nil
}

type Partition struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Offsets []int64 `protobuf:"varint,1,rep,packed,name=offsets,proto3" json:"offsets,omitempty"`
	Before  int64   `protobuf:"varint,2,opt,name=before,proto3" json:"before,omitempty"`
}

func (x *Partition) Reset() {
	*x = Partition{}
	if protoimpl.UnsafeEnabled {
		mi := &file_wkafka_wkafka_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Partition) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Partition) ProtoMessage() {}

func (x *Partition) ProtoReflect() protoreflect.Message {
	mi := &file_wkafka_wkafka_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Partition.ProtoReflect.Descriptor instead.
func (*Partition) Descriptor() ([]byte, []int) {
	return file_wkafka_wkafka_proto_rawDescGZIP(), []int{2}
}

func (x *Partition) GetOffsets() []int64 {
	if x != nil {
		return x.Offsets
	}
	return nil
}

func (x *Partition) GetBefore() int64 {
	if x != nil {
		return x.Before
	}
	return 0
}

type Response struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Message string `protobuf:"bytes,1,opt,name=message,proto3" json:"message,omitempty"`
}

func (x *Response) Reset() {
	*x = Response{}
	if protoimpl.UnsafeEnabled {
		mi := &file_wkafka_wkafka_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Response) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Response) ProtoMessage() {}

func (x *Response) ProtoReflect() protoreflect.Message {
	mi := &file_wkafka_wkafka_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Response.ProtoReflect.Descriptor instead.
func (*Response) Descriptor() ([]byte, []int) {
	return file_wkafka_wkafka_proto_rawDescGZIP(), []int{3}
}

func (x *Response) GetMessage() string {
	if x != nil {
		return x.Message
	}
	return ""
}

var File_wkafka_wkafka_proto protoreflect.FileDescriptor

var file_wkafka_wkafka_proto_rawDesc = []byte{
	0x0a, 0x13, 0x77, 0x6b, 0x61, 0x66, 0x6b, 0x61, 0x2f, 0x77, 0x6b, 0x61, 0x66, 0x6b, 0x61, 0x2e,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x06, 0x77, 0x6b, 0x61, 0x66, 0x6b, 0x61, 0x22, 0xf6, 0x01,
	0x0a, 0x11, 0x43, 0x72, 0x65, 0x61, 0x74, 0x65, 0x53, 0x6b, 0x69, 0x70, 0x52, 0x65, 0x71, 0x75,
	0x65, 0x73, 0x74, 0x12, 0x3d, 0x0a, 0x06, 0x74, 0x6f, 0x70, 0x69, 0x63, 0x73, 0x18, 0x01, 0x20,
	0x03, 0x28, 0x0b, 0x32, 0x25, 0x2e, 0x77, 0x6b, 0x61, 0x66, 0x6b, 0x61, 0x2e, 0x43, 0x72, 0x65,
	0x61, 0x74, 0x65, 0x53, 0x6b, 0x69, 0x70, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x2e, 0x54,
	0x6f, 0x70, 0x69, 0x63, 0x73, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x52, 0x06, 0x74, 0x6f, 0x70, 0x69,
	0x63, 0x73, 0x12, 0x2a, 0x0a, 0x06, 0x6f, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x18, 0x02, 0x20, 0x01,
	0x28, 0x0e, 0x32, 0x12, 0x2e, 0x77, 0x6b, 0x61, 0x66, 0x6b, 0x61, 0x2e, 0x53, 0x6b, 0x69, 0x70,
	0x4f, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x06, 0x6f, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x2c,
	0x0a, 0x12, 0x65, 0x6e, 0x61, 0x62, 0x6c, 0x65, 0x5f, 0x6d, 0x61, 0x69, 0x6e, 0x5f, 0x74, 0x6f,
	0x70, 0x69, 0x63, 0x73, 0x18, 0x03, 0x20, 0x01, 0x28, 0x08, 0x52, 0x10, 0x65, 0x6e, 0x61, 0x62,
	0x6c, 0x65, 0x4d, 0x61, 0x69, 0x6e, 0x54, 0x6f, 0x70, 0x69, 0x63, 0x73, 0x1a, 0x48, 0x0a, 0x0b,
	0x54, 0x6f, 0x70, 0x69, 0x63, 0x73, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x12, 0x10, 0x0a, 0x03, 0x6b,
	0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x23, 0x0a,
	0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x0d, 0x2e, 0x77,
	0x6b, 0x61, 0x66, 0x6b, 0x61, 0x2e, 0x54, 0x6f, 0x70, 0x69, 0x63, 0x52, 0x05, 0x76, 0x61, 0x6c,
	0x75, 0x65, 0x3a, 0x02, 0x38, 0x01, 0x22, 0x98, 0x01, 0x0a, 0x05, 0x54, 0x6f, 0x70, 0x69, 0x63,
	0x12, 0x3d, 0x0a, 0x0a, 0x70, 0x61, 0x72, 0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x18, 0x01,
	0x20, 0x03, 0x28, 0x0b, 0x32, 0x1d, 0x2e, 0x77, 0x6b, 0x61, 0x66, 0x6b, 0x61, 0x2e, 0x54, 0x6f,
	0x70, 0x69, 0x63, 0x2e, 0x50, 0x61, 0x72, 0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x45, 0x6e,
	0x74, 0x72, 0x79, 0x52, 0x0a, 0x70, 0x61, 0x72, 0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x1a,
	0x50, 0x0a, 0x0f, 0x50, 0x61, 0x72, 0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x45, 0x6e, 0x74,
	0x72, 0x79, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x05, 0x52,
	0x03, 0x6b, 0x65, 0x79, 0x12, 0x27, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x02, 0x20,
	0x01, 0x28, 0x0b, 0x32, 0x11, 0x2e, 0x77, 0x6b, 0x61, 0x66, 0x6b, 0x61, 0x2e, 0x50, 0x61, 0x72,
	0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x3a, 0x02, 0x38,
	0x01, 0x22, 0x3d, 0x0a, 0x09, 0x50, 0x61, 0x72, 0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x18,
	0x0a, 0x07, 0x6f, 0x66, 0x66, 0x73, 0x65, 0x74, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x03, 0x52,
	0x07, 0x6f, 0x66, 0x66, 0x73, 0x65, 0x74, 0x73, 0x12, 0x16, 0x0a, 0x06, 0x62, 0x65, 0x66, 0x6f,
	0x72, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x03, 0x52, 0x06, 0x62, 0x65, 0x66, 0x6f, 0x72, 0x65,
	0x22, 0x24, 0x0a, 0x08, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x18, 0x0a, 0x07,
	0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x6d,
	0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x2a, 0x25, 0x0a, 0x0a, 0x53, 0x6b, 0x69, 0x70, 0x4f, 0x70,
	0x74, 0x69, 0x6f, 0x6e, 0x12, 0x0a, 0x0a, 0x06, 0x41, 0x50, 0x50, 0x45, 0x4e, 0x44, 0x10, 0x00,
	0x12, 0x0b, 0x0a, 0x07, 0x52, 0x45, 0x50, 0x4c, 0x41, 0x43, 0x45, 0x10, 0x01, 0x32, 0x44, 0x0a,
	0x0d, 0x57, 0x6b, 0x61, 0x66, 0x6b, 0x61, 0x53, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x12, 0x33,
	0x0a, 0x04, 0x53, 0x6b, 0x69, 0x70, 0x12, 0x19, 0x2e, 0x77, 0x6b, 0x61, 0x66, 0x6b, 0x61, 0x2e,
	0x43, 0x72, 0x65, 0x61, 0x74, 0x65, 0x53, 0x6b, 0x69, 0x70, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73,
	0x74, 0x1a, 0x10, 0x2e, 0x77, 0x6b, 0x61, 0x66, 0x6b, 0x61, 0x2e, 0x52, 0x65, 0x73, 0x70, 0x6f,
	0x6e, 0x73, 0x65, 0x42, 0x84, 0x01, 0x0a, 0x0a, 0x63, 0x6f, 0x6d, 0x2e, 0x77, 0x6b, 0x61, 0x66,
	0x6b, 0x61, 0x42, 0x0b, 0x57, 0x6b, 0x61, 0x66, 0x6b, 0x61, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x50,
	0x01, 0x5a, 0x31, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x77, 0x6f,
	0x72, 0x6c, 0x64, 0x6c, 0x69, 0x6e, 0x65, 0x2d, 0x67, 0x6f, 0x2f, 0x77, 0x6b, 0x61, 0x66, 0x6b,
	0x61, 0x2f, 0x68, 0x61, 0x6e, 0x64, 0x6c, 0x65, 0x72, 0x2f, 0x67, 0x65, 0x6e, 0x2f, 0x77, 0x6b,
	0x61, 0x66, 0x6b, 0x61, 0xa2, 0x02, 0x03, 0x57, 0x58, 0x58, 0xaa, 0x02, 0x06, 0x57, 0x6b, 0x61,
	0x66, 0x6b, 0x61, 0xca, 0x02, 0x06, 0x57, 0x6b, 0x61, 0x66, 0x6b, 0x61, 0xe2, 0x02, 0x12, 0x57,
	0x6b, 0x61, 0x66, 0x6b, 0x61, 0x5c, 0x47, 0x50, 0x42, 0x4d, 0x65, 0x74, 0x61, 0x64, 0x61, 0x74,
	0x61, 0xea, 0x02, 0x06, 0x57, 0x6b, 0x61, 0x66, 0x6b, 0x61, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x33,
}

var (
	file_wkafka_wkafka_proto_rawDescOnce sync.Once
	file_wkafka_wkafka_proto_rawDescData = file_wkafka_wkafka_proto_rawDesc
)

func file_wkafka_wkafka_proto_rawDescGZIP() []byte {
	file_wkafka_wkafka_proto_rawDescOnce.Do(func() {
		file_wkafka_wkafka_proto_rawDescData = protoimpl.X.CompressGZIP(file_wkafka_wkafka_proto_rawDescData)
	})
	return file_wkafka_wkafka_proto_rawDescData
}

var file_wkafka_wkafka_proto_enumTypes = make([]protoimpl.EnumInfo, 1)
var file_wkafka_wkafka_proto_msgTypes = make([]protoimpl.MessageInfo, 6)
var file_wkafka_wkafka_proto_goTypes = []any{
	(SkipOption)(0),           // 0: wkafka.SkipOption
	(*CreateSkipRequest)(nil), // 1: wkafka.CreateSkipRequest
	(*Topic)(nil),             // 2: wkafka.Topic
	(*Partition)(nil),         // 3: wkafka.Partition
	(*Response)(nil),          // 4: wkafka.Response
	nil,                       // 5: wkafka.CreateSkipRequest.TopicsEntry
	nil,                       // 6: wkafka.Topic.PartitionsEntry
}
var file_wkafka_wkafka_proto_depIdxs = []int32{
	5, // 0: wkafka.CreateSkipRequest.topics:type_name -> wkafka.CreateSkipRequest.TopicsEntry
	0, // 1: wkafka.CreateSkipRequest.option:type_name -> wkafka.SkipOption
	6, // 2: wkafka.Topic.partitions:type_name -> wkafka.Topic.PartitionsEntry
	2, // 3: wkafka.CreateSkipRequest.TopicsEntry.value:type_name -> wkafka.Topic
	3, // 4: wkafka.Topic.PartitionsEntry.value:type_name -> wkafka.Partition
	1, // 5: wkafka.WkafkaService.Skip:input_type -> wkafka.CreateSkipRequest
	4, // 6: wkafka.WkafkaService.Skip:output_type -> wkafka.Response
	6, // [6:7] is the sub-list for method output_type
	5, // [5:6] is the sub-list for method input_type
	5, // [5:5] is the sub-list for extension type_name
	5, // [5:5] is the sub-list for extension extendee
	0, // [0:5] is the sub-list for field type_name
}

func init() { file_wkafka_wkafka_proto_init() }
func file_wkafka_wkafka_proto_init() {
	if File_wkafka_wkafka_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_wkafka_wkafka_proto_msgTypes[0].Exporter = func(v any, i int) any {
			switch v := v.(*CreateSkipRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_wkafka_wkafka_proto_msgTypes[1].Exporter = func(v any, i int) any {
			switch v := v.(*Topic); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_wkafka_wkafka_proto_msgTypes[2].Exporter = func(v any, i int) any {
			switch v := v.(*Partition); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_wkafka_wkafka_proto_msgTypes[3].Exporter = func(v any, i int) any {
			switch v := v.(*Response); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_wkafka_wkafka_proto_rawDesc,
			NumEnums:      1,
			NumMessages:   6,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_wkafka_wkafka_proto_goTypes,
		DependencyIndexes: file_wkafka_wkafka_proto_depIdxs,
		EnumInfos:         file_wkafka_wkafka_proto_enumTypes,
		MessageInfos:      file_wkafka_wkafka_proto_msgTypes,
	}.Build()
	File_wkafka_wkafka_proto = out.File
	file_wkafka_wkafka_proto_rawDesc = nil
	file_wkafka_wkafka_proto_goTypes = nil
	file_wkafka_wkafka_proto_depIdxs = nil
}
